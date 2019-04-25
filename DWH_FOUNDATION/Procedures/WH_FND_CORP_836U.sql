--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_836U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_836U" 
  (
    p_forall_limit IN INTEGER ,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        February 2009
  --  Author:      Wendy Lyttle
  --  Purpose:     Load sales budget in the foundation layer
  --               with input ex staging table ex excel spread sheet.
  --  Tables:      Input  - stg_excel_fin_budg_cpy
  --               Output - fnd_rtl_loc_dept_wk_fin_budg
  --  Packages:    constants, dwh_log, dwh_valid
  -- dwh_foundation.temp_fnd_rtl_lc_dpt_wk_fn_bdg
  --  Maintenance:
  --  24 Feb 2009 - TD 861 - remove field sales_hl_ho_fcst
  -- 11 jul 2009 - td 1902  - WWW Budget Load Issue/ Error
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := 3000;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_hospital      CHAR(1) := 'N';
  g_record_type  varchar2(100) :='';
  g_hospital_text stg_excel_fin_budg_hsp.sys_process_msg%type;
--  g_rec_out fnd_rtl_loc_dept_wk_fin_budg%rowtype;
  g_rec_out temp_fnd_rtl_lc_dpt_wk_fn_bdg%rowtype;
  g_rec_in stg_excel_fin_budg_cpy%rowtype;
  g_found      BOOLEAN;
  g_insert_rec BOOLEAN;
  g_fin_year_no dim_calendar.fin_year_no%type;
  g_fin_week_no dim_calendar.fin_week_no%type;
  --g_date              date          := to_char(sysdate,('dd mon yyyy'));
  g_date DATE := TRUNC(sysdate) ;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_836U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD SALES BUDGET EX FINANCE SPREADSHEET';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For input bulk collect --
type stg_array
IS
  TABLE OF stg_excel_fin_budg_cpy%rowtype;
  a_stg_input stg_array;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF temp_fnd_rtl_lc_dpt_wk_fn_bdg%rowtype INDEX BY binary_integer;
--  TABLE OF fnd_rtl_loc_dept_wk_fin_budg%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF temp_fnd_rtl_lc_dpt_wk_fn_bdg%rowtype INDEX BY binary_integer;
--  TABLE OF fnd_rtl_loc_dept_wk_fin_budg%rowtype INDEX BY binary_integer;
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
  TABLE OF stg_excel_fin_budg_cpy.sys_source_batch_id%type INDEX BY binary_integer;
type staging_array2
IS
  TABLE OF stg_excel_fin_budg_cpy.sys_source_sequence_no%type INDEX BY binary_integer;
  a_staging1 staging_array1;
  a_staging2 staging_array2;
  a_empty_set_s1 staging_array1;
  a_empty_set_s2 staging_array2;
  a_count_stg INTEGER := 0;
  --
  -- Adding a lookup to the calendar here reduces the no. of reads later on.
  -- This is the exception and not the rule.
  -- Allowing left outer join will ensure that all staging records are
  -- selected where sys_process_code = 'N' regardless of
  -- the postdate being correct. The validation of the postdate will be handled
  -- in the local_address_variables.
  --
  --****
  -- A pivot table could not be used here as the record_types sometimes
  -- used the incl_vat value and sometimes the excl_vat value
  ---
  CURSOR c_stg_excel_loc_dept_wk
  IS
     SELECT *
       FROM stg_excel_fin_budg_cpy
 --             where post_date = '29/JUN/09'
 --      and location_no = 3001
     WHERE sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;
  -- order by only where sequencing is essential to the correct loading of data
  --**************************************************************************************************
  -- Process, transform and validate the data read from the input interface
  --**************************************************************************************************
PROCEDURE local_address_variables
AS
  v_count NUMBER := 0;
BEGIN
  g_hospital                              := 'N';
  g_rec_out                               := null;
  g_rec_out.location_no                   := g_rec_in.location_no;
  g_rec_out.department_no                 := g_rec_in.department_no;
  g_rec_out.record_type                   := g_rec_in.record_type;

  -- ETL yet to be designed for THIS FIELD
  --g_rec_out.sales_hl_ho_fcst              := 0;
  --
--  dbms_output.put_line('lav - '||g_rec_in.LOCATION_NO
--||' lav - '||g_rec_in.DEPARTMENT_NO
--||' lav - '||g_rec_in.post_date
----||' lav - '||g_rec_in.record_type
--||' lav - '||G_REC_IN.FIELD_VALUE_EXCL_VAT
--||' lav - '||G_REC_IN.FIELD_VALUE_INCL_VAT
--);
  g_rec_out.source_data_status_code       := g_rec_in.source_data_status_code;
  g_rec_out.last_updated_date             := g_date;

  IF G_REC_IN.RECORD_TYPE          = 'SALES_BUDGET' THEN
    g_rec_out.SALES_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.SALES_BUDGET_INCL_VAT := G_REC_IN.FIELD_VALUE_INCL_VAT;
    g_rec_out.sales_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.sales_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'ONLINE_SALES_BUDGET' THEN
    g_rec_out.ONLINE_SALES_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.online_sales_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.online_sales_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE                     = 'WASTE_REC_BUDGET' THEN
    g_rec_out.WASTE_RECOVERY_BUDGET_INCL_VAT := G_REC_IN.FIELD_VALUE_INCL_VAT;
  END IF;

  IF G_REC_IN.RECORD_TYPE                   = 'WWCARD_SALES_BUDGET' THEN
    g_rec_out.wwcard_sales_budget_incl_vat := G_REC_IN.FIELD_VALUE_INCL_VAT;
    g_rec_out.wwcard_sales_budget_ivat_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.wwcard_sales_budget_ivat_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE  = 'FTE_BUDGET_ACTUAL' THEN
    g_rec_out.FIN_NUM_FTE := G_REC_IN.FIELD_VALUE_INCL_VAT;
    g_rec_out.FIN_BUDGET_FTE_AMT := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.fin_num_fte_mzar           := g_rec_in.FIELD_VALUE_INCL_VAT;
    g_rec_out.fin_budget_fte_amt_mzar    := g_rec_in.field_value_mzar;
    g_rec_out.fin_num_fte_afr            := g_rec_in.FIELD_VALUE_INCL_VAT;
    g_rec_out.fin_budget_fte_amt_afr     := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE         = 'SALES_UNITS_BUDGET' THEN
    g_rec_out.SALES_UNITS_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.sales_units_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.sales_units_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE      = 'TRANSACTIONS_BUDGET' THEN
    g_rec_out.NUM_TRAN_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.num_tran_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.num_tran_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'CUSTOMERS_CHBFBUDGET' THEN
    g_rec_out.NUM_TRAN_998_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.num_tran_998_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.num_tran_998_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'CUSTOMERS_BUDGET' THEN
    g_rec_out.NUM_TRAN_999_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.num_tran_999_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.num_tran_999_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE        = 'WASTE_COST_BUDGET' THEN
    g_rec_out.WASTE_COST_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.waste_cost_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.waste_cost_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE            = 'SHRINKAGE_COST_BUDGET' THEN
    g_rec_out.SHRINKAGE_COST_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.shrinkage_cost_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.shrinkage_cost_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'SALES_MARGIN_BUDGET' THEN
    g_rec_out.SALES_MARGIN_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.sales_margin_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.sales_margin_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE    = 'SALES_PLAN_BUDGET' THEN
    g_rec_out.SALES_PLANNED := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.sales_planned_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.sales_planned_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE        = 'NET_MARGIN_BUDGET' THEN
    g_rec_out.NET_MARGIN_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.net_margin_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.net_margin_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'GROSS_PROFIT_BUDGET' THEN
    g_rec_out.GROSS_PROFIT_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.gross_profit_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.gross_profit_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE           = 'FRONT_OFFICE_NUM_FTE_ACTUAL' THEN
    g_rec_out.FRONT_OFFICE_NUM_FTE := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.front_office_num_fte_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.front_office_num_fte_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'BACK_OFFICE_NUM_FTE_ACTUAL' THEN
    g_rec_out.BACK_OFFICE_NUM_FTE := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.sales_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.sales_budget_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'CONTRACTORS_NUM_FTE_ACTUAL' THEN
    g_rec_out.CONTRACTORS_NUM_FTE := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.back_office_num_fte_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.back_office_num_fte_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE   = 'CASH_REFUNDS_ACTUAL' THEN
    g_rec_out.CASH_REFUNDS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.cash_refunds_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.cash_refunds_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE      = 'WW_CARD_REFUNDS_ACTUAL' THEN
    g_rec_out.WW_CARD_REFUNDS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.ww_card_refunds_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.ww_card_refunds_afr      := g_rec_in.field_value_local;
   END IF;

  IF G_REC_IN.RECORD_TYPE        = 'BANK_CARD_REFUNDS_ACTUAL' THEN
    g_rec_out.BANK_CARD_REFUNDS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.bank_card_refunds_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.bank_card_refunds_afr      := g_rec_in.field_value_local;
  ELSE
      g_rec_out.BANK_CARD_REFUNDS := 0;
  END IF;

  IF G_REC_IN.RECORD_TYPE             = 'CREDIT_VOUCHER_REFUNDS_ACTUAL' THEN
    g_rec_out.CREDIT_VOUCHER_REFUNDS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.credit_voucher_refunds_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.credit_voucher_refunds_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE        = 'GIFT_CARD_REFUNDS_ACTUAL' THEN
    g_rec_out.GIFT_CARD_REFUNDS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.gift_card_refunds_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.gift_card_refunds_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE            = 'CASH_TILL_DISCREPANCY_ACTUAL' THEN
    g_rec_out.CASH_TILL_DISCREPANCY := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.cash_till_discrepancy_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.cash_till_discrepancy_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE               = 'CHEQUES_TILL_DISCREPANCY_ACTUAL' THEN
    g_rec_out.CHEQUES_TILL_DISCREPANCY := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.cheques_till_discrepancy_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.cheques_till_discrepancy_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE               = 'VOUCHER_TILL_DISCREPANCY_ACTUAL' THEN
    g_rec_out.VOUCHER_TILL_DISCREPANCY := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.voucher_till_discrepancy_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.voucher_till_discrepancy_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE                = 'NON_CASH_TILL_DISCREPANCY_ACTUAL' THEN
    g_rec_out.NON_CASH_TILL_DISCREPANCY := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.non_cash_till_discrepancy_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.non_cash_till_discrepancy_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE        = 'FLOAT_DISCREPANCY_ACTUAL' THEN
    g_rec_out.FLOAT_DISCREPANCY := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.float_discrepancy_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.float_discrepancy_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE    = 'FLOAT_HOLDING_ACTUAL' THEN
    g_rec_out.FLOAT_HOLDING := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.FLOAT_HOLDING_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.FLOAT_HOLDING_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE         = 'INDEPENDENT_COUNTS_ACTUAL' THEN
    g_rec_out.INDEPENDENT_COUNTS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.INDEPENDENT_COUNTS_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.INDEPENDENT_COUNTS_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE       = 'HAND_OVER_COUNTS_ACTUAL' THEN
    g_rec_out.HAND_OVER_COUNTS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.HAND_OVER_COUNTS_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.HAND_OVER_COUNTS_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE  = 'SELF_COUNTS_ACTUAL' THEN
    g_rec_out.SELF_COUNTS := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.SELF_COUNTS_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.SELF_COUNTS_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE         = 'WASTE_REC_BUDGET' THEN
    g_rec_out.WASTE_RECOVERY_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.WASTE_RECOVERY_BUDGET_INCL_VAT := G_REC_IN.FIELD_VALUE_INCL_VAT;
    g_rec_out.waste_recovery_budgt_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.waste_recovery_budgt_afr      := g_rec_in.field_value_local;
  END IF;

  IF G_REC_IN.RECORD_TYPE          = 'ONLINE_SALES_BUDGET' THEN
    g_rec_out.ONLINE_SALES_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.ONLINE_SALES_BUDGET_INCL_VAT := G_REC_IN.FIELD_VALUE_INCL_VAT;
    g_rec_out.online_sales_budget_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.online_sales_budget_afr      := g_rec_in.field_value_local;
  END IF;
  
 -- SPECIAL DEPT REVENUE BUDGETS / OUTSOURCE
  IF G_REC_IN.RECORD_TYPE          = 'SPEC_DEPT_REVENUE_BUDGET' THEN
    g_rec_out.SPEC_DEPT_REVENUE_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.SPEC_DPT_REV_BUDGET_INCL_VAT := G_REC_IN.FIELD_VALUE_INCL_VAT;
    g_rec_out.SPEC_DEPT_REVENUE_BUDGET_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.SPEC_DEPT_REVENUE_BUDGET_afr      := g_rec_in.field_value_local;
  END IF;
  
    IF G_REC_IN.RECORD_TYPE          = 'SPEC_DEPT_REVENUE_QTY_BUDGET' THEN
    g_rec_out.SPEC_DEPT_REVENUE_QTY_BUDGET := G_REC_IN.FIELD_VALUE_EXCL_VAT;
    g_rec_out.SPEC_DPT_REV_QTY_BUDGT_INC_VAT := G_REC_IN.FIELD_VALUE_INCL_VAT;
    g_rec_out.SPEC_DEPT_REVENUE_QTY_BDG_mzar     := g_rec_in.field_value_mzar;
    g_rec_out.SPEC_DEPT_REVENUE_QTY_BDG_afr      := g_rec_in.field_value_local;
  END IF;
  
  IF NOT dwh_valid.fnd_location(g_rec_out.location_no) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_location_not_found;
    l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
    dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
    RETURN;
  END IF;
  IF NOT dwh_valid.fnd_department(g_rec_out.department_no) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_dept_not_found;
    l_text          := dwh_constants.vc_dept_not_found||g_rec_out.location_no ;
    dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
    RETURN;
  END IF;
  IF NOT dwh_valid.fnd_calendar(g_rec_in.post_date) THEN
    g_hospital      := 'Y';
    g_hospital_text := dwh_constants.vc_date_not_found;
    l_text          := dwh_constants.vc_date_not_found||g_rec_in.post_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  END IF;

  if not dwh_valid.dim_dept_child_hierarchy(g_rec_out.department_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_dept_child_not_found;
     l_text := dwh_constants.vc_dept_child_not_found||' '||g_rec_out.department_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  end if;

    SELECT fin_year_no,
    fin_week_no
     INTO g_fin_year_no,
    g_fin_week_no
     FROM dim_calendar
    WHERE calendar_date = g_rec_in.post_date;

  g_rec_out.fin_year_no := g_fin_year_no;
  g_rec_out.fin_week_no := g_fin_week_no;
  -- Validate fin_year_no and fin_week_no against FND_CALENDAR
  --  SELECT
  --  COUNT(1)
  --     INTO
  --   v_count
  --    FROM
  --   fnd_calendar
  --    WHERE
  --   fin_year_no      = g_rec_in.fin_year_no
  --  AND fin_week_no    = g_rec_in.fin_week_no;
  -- this_week_start_date = g_rec_in.this_week_start_date;
  --  IF v_count         = 0 THEN
  --   g_hospital      := 'Y';
  --    g_hospital_text := 'INVALID FIN YEAR OR WEEK - FND_CALENDAR CONTAINS VALID VALUES';
  --   l_text          := 'INVALID FIN YEAR OR WEEK - FND_CALENDAR CONTAINS VALID VALUES'||g_rec_out.fin_year_no||' '||g_rec_out.fin_week_no;
  --    dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  --  END IF;
EXCEPTION
WHEN OTHERS THEN
  --dbms_output.put_line('rec_type='||G_REC_IN.RECORD_TYPE||' val='||G_REC_IN.FIELD_VALUE_EXCL_VAT||' val='||G_REC_IN.FIELD_VALUE_INCL_VAT);
  l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  raise;
END local_address_variables;
--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
PROCEDURE local_write_hospital
AS
BEGIN
--dbms_output.put_line('hsp - '||g_rec_in.LOCATION_NO
--||' hsp - '||g_rec_in.DEPARTMENT_NO
--||' hsp - '||g_rec_in.post_date
--);
  g_rec_in.sys_load_date        := sysdate;
  g_rec_in.sys_load_system_name := 'DWH';
  g_rec_in.sys_process_code     := 'Y';
  g_rec_in.sys_process_msg      := g_hospital_text;
   INSERT INTO stg_excel_fin_budg_hsp VALUES g_rec_in;

  g_recs_hospital := g_recs_hospital + sql%rowcount;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lh_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name ,SQLCODE ,l_message
  )
  ;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_lh_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name ,SQLCODE ,l_message
  )
  ;
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
   INSERT INTO fnd_rtl_loc_dept_wk_fin_budg VALUES
    (a_tbl_insert(i).LOCATION_NO,
a_tbl_insert(i).DEPARTMENT_NO,
a_tbl_insert(i).FIN_YEAR_NO,
a_tbl_insert(i).FIN_WEEK_NO,
a_tbl_insert(i).ONLINE_SALES_BUDGET,
a_tbl_insert(i).ONLINE_SALES_BUDGET_INCL_VAT,
a_tbl_insert(i).SALES_BUDGET,
a_tbl_insert(i).SALES_BUDGET_INCL_VAT,
a_tbl_insert(i).WASTE_RECOVERY_BUDGET,
a_tbl_insert(i).WASTE_RECOVERY_BUDGET_INCL_VAT,
a_tbl_insert(i).WWCARD_SALES_BUDGET_INCL_VAT,
a_tbl_insert(i).FIN_NUM_FTE,
a_tbl_insert(i).FIN_BUDGET_FTE_AMT,
a_tbl_insert(i).SALES_UNITS_BUDGET,
a_tbl_insert(i).NUM_TRAN_BUDGET,
a_tbl_insert(i).NUM_TRAN_998_BUDGET,
a_tbl_insert(i).NUM_TRAN_999_BUDGET,
a_tbl_insert(i).WASTE_COST_BUDGET,
a_tbl_insert(i).SHRINKAGE_COST_BUDGET,
a_tbl_insert(i).SALES_MARGIN_BUDGET,
a_tbl_insert(i).SALES_PLANNED,
a_tbl_insert(i).NET_MARGIN_BUDGET,
a_tbl_insert(i).GROSS_PROFIT_BUDGET,
a_tbl_insert(i).FRONT_OFFICE_NUM_FTE,
a_tbl_insert(i).BACK_OFFICE_NUM_FTE,
a_tbl_insert(i).CONTRACTORS_NUM_FTE,
a_tbl_insert(i).CASH_REFUNDS,
a_tbl_insert(i).WW_CARD_REFUNDS,
a_tbl_insert(i).BANK_CARD_REFUNDS,
a_tbl_insert(i).CREDIT_VOUCHER_REFUNDS,
a_tbl_insert(i).GIFT_CARD_REFUNDS,
a_tbl_insert(i).CASH_TILL_DISCREPANCY,
a_tbl_insert(i).CHEQUES_TILL_DISCREPANCY,
a_tbl_insert(i).VOUCHER_TILL_DISCREPANCY,
a_tbl_insert(i).NON_CASH_TILL_DISCREPANCY,
a_tbl_insert(i).FLOAT_DISCREPANCY,
a_tbl_insert(i).FLOAT_HOLDING,
a_tbl_insert(i).INDEPENDENT_COUNTS,
a_tbl_insert(i).HAND_OVER_COUNTS,
a_tbl_insert(i).SELF_COUNTS,
a_tbl_insert(i).SOURCE_DATA_STATUS_CODE,
a_tbl_insert(i).LAST_UPDATED_DATE,
a_tbl_insert(i).SPEC_DEPT_REVENUE_BUDGET,          --ADDED JULY 2015
a_tbl_insert(i).SPEC_DPT_REV_BUDGET_INCL_VAT,      --ADDED JULY 2015
a_tbl_insert(i).SPEC_DEPT_REVENUE_QTY_BUDGET,
a_tbl_insert(i).SPEC_DPT_REV_QTY_BUDGT_INC_VAT,
-- multi currency --
a_tbl_insert(i).	online_sales_budget_mzar,
a_tbl_insert(i).	online_sales_budget_ivat_mzar,
a_tbl_insert(i).	sales_budget_mzar,
a_tbl_insert(i).	sales_budget_incl_vat_mzar,
a_tbl_insert(i).	waste_recovery_budgt_mzar,
a_tbl_insert(i).	waste_recovery_budgt_ivat_mzar,
a_tbl_insert(i).	wwcard_sales_budget_ivat_mzar,
a_tbl_insert(i).	fin_num_fte_mzar,
a_tbl_insert(i).	fin_budget_fte_amt_mzar,
a_tbl_insert(i).	sales_units_budget_mzar,
a_tbl_insert(i).	num_tran_budget_mzar,
a_tbl_insert(i).	num_tran_998_budget_mzar,
a_tbl_insert(i).	num_tran_999_budget_mzar,
a_tbl_insert(i).	waste_cost_budget_mzar,
a_tbl_insert(i).	shrinkage_cost_budget_mzar,
a_tbl_insert(i).	sales_margin_budget_mzar,
a_tbl_insert(i).	sales_planned_mzar,
a_tbl_insert(i).	net_margin_budget_mzar,
a_tbl_insert(i).	gross_profit_budget_mzar,
a_tbl_insert(i).	front_office_num_fte_mzar,
a_tbl_insert(i).	back_office_num_fte_mzar,
a_tbl_insert(i).	contractors_num_fte_mzar,
a_tbl_insert(i).	cash_refunds_mzar,
a_tbl_insert(i).	ww_card_refunds_mzar,
a_tbl_insert(i).	bank_card_refunds_mzar,
a_tbl_insert(i).	credit_voucher_refunds_mzar,
a_tbl_insert(i).	gift_card_refunds_mzar,
a_tbl_insert(i).	cash_till_discrepancy_mzar,
a_tbl_insert(i).	cheques_till_discrepancy_mzar,
a_tbl_insert(i).	voucher_till_discrepancy_mzar,
a_tbl_insert(i).	non_cash_till_discrepancy_mzar,
a_tbl_insert(i).	float_discrepancy_mzar,
a_tbl_insert(i).	float_holding_mzar,
a_tbl_insert(i).	independent_counts_mzar,
a_tbl_insert(i).	hand_over_counts_mzar,
a_tbl_insert(i).	self_counts_mzar,
a_tbl_insert(i).	spec_dept_revenue_budget_mzar,
a_tbl_insert(i).	spec_dpt_rev_budget_ivat_mzar,
a_tbl_insert(i).	spec_dept_revenue_qty_bdg_mzar,
a_tbl_insert(i).	spec_dpt_rev_qty_bdg_ivat_mzar,
a_tbl_insert(i).	online_sales_budget_afr,
a_tbl_insert(i).	online_sales_budget_ivat_afr,
a_tbl_insert(i).	sales_budget_afr,
a_tbl_insert(i).	sales_budget_incl_vat_afr,
a_tbl_insert(i).	waste_recovery_budgt_afr,
a_tbl_insert(i).	waste_recovery_budgt_ivat_afr,
a_tbl_insert(i).	wwcard_sales_budget_ivat_afr,
a_tbl_insert(i).	fin_num_fte_afr,
a_tbl_insert(i).	fin_budget_fte_amt_afr,
a_tbl_insert(i).	sales_units_budget_afr,
a_tbl_insert(i).	num_tran_budget_afr,
a_tbl_insert(i).	num_tran_998_budget_afr,
a_tbl_insert(i).	num_tran_999_budget_afr,
a_tbl_insert(i).	waste_cost_budget_afr,
a_tbl_insert(i).	shrinkage_cost_budget_afr,
a_tbl_insert(i).	sales_margin_budget_afr,
a_tbl_insert(i).	sales_planned_afr,
a_tbl_insert(i).	net_margin_budget_afr,
a_tbl_insert(i).	gross_profit_budget_afr,
a_tbl_insert(i).	front_office_num_fte_afr,
a_tbl_insert(i).	back_office_num_fte_afr,
a_tbl_insert(i).	contractors_num_fte_afr,
a_tbl_insert(i).	cash_refunds_afr,
a_tbl_insert(i).	ww_card_refunds_afr,
a_tbl_insert(i).	bank_card_refunds_afr,
a_tbl_insert(i).	credit_voucher_refunds_afr,
a_tbl_insert(i).	gift_card_refunds_afr,
a_tbl_insert(i).	cash_till_discrepancy_afr,
a_tbl_insert(i).	cheques_till_discrepancy_afr,
a_tbl_insert(i).	voucher_till_discrepancy_afr,
a_tbl_insert(i).	non_cash_till_discrepancy_afr,
a_tbl_insert(i).	float_discrepancy_afr,
a_tbl_insert(i).	float_holding_afr,
a_tbl_insert(i).	independent_counts_afr,
a_tbl_insert(i).	hand_over_counts_afr,
a_tbl_insert(i).	self_counts_afr,
a_tbl_insert(i).	spec_dept_revenue_budget_afr,
a_tbl_insert(i).	spec_dpt_rev_budget_ivat_afr,
a_tbl_insert(i).	spec_dept_revenue_qty_bdg_afr,
a_tbl_insert(i).	spec_dpt_rev_qty_bdg_ivat_afr


    ) ;

  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name ,SQLCODE ,l_message
  )
  ;
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions
    (
      i
    )
    .error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm
    (
      - sql%bulk_exceptions(i) .error_code
    )
    || ' '||a_tbl_insert
    (
      g_error_index
    )
    .location_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .department_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .fin_year_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .fin_week_no;
    dwh_log.record_error
    (
      l_module_name ,SQLCODE ,l_message
    )
    ;
  END LOOP;
  raise;
END local_bulk_insert;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin


   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
 update fnd_rtl_loc_dept_wk_fin_budg
      set sales_budget                  = case a_tbl_update(i).record_type
                                          when 'SALES_BUDGET'
                                          then a_tbl_update(i).sales_budget
                                          else sales_budget end ,
      sales_budget_incl_vat             = case a_tbl_update(i).record_type
                                          when 'SALES_BUDGET'
                                          then a_tbl_update(i).SALES_BUDGET_INCL_VAT
                                          else SALES_BUDGET_INCL_VAT end,
      back_office_num_fte               = case a_tbl_update(i).record_type
                                          when 'BACK_OFFICE_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).back_office_num_fte
                                          else back_office_num_fte end,
      bank_card_refunds                 = case a_tbl_update(i).record_type
                                          when 'BANK_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).bank_card_refunds
                                          else bank_card_refunds end,
      cash_refunds                      = case a_tbl_update(i).record_type
                                          when 'CASH_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).cash_refunds
                                          else cash_refunds end,
      cash_till_discrepancy             = case a_tbl_update(i).record_type
                                          when 'CASH_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).cash_till_discrepancy
                                          else cash_till_discrepancy end,
      cheques_till_discrepancy          = case a_tbl_update(i).record_type
                                          when 'CHEQUES_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).cheques_till_discrepancy
                                          else cheques_till_discrepancy end,
      contractors_num_fte               = case a_tbl_update(i).record_type
                                          when 'CONTRACTORS_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).contractors_num_fte
                                          else contractors_num_fte end,
      credit_voucher_refunds            = case a_tbl_update(i).record_type
                                          when 'CREDIT_VOUCHER_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).credit_voucher_refunds
                                          else credit_voucher_refunds end,
      fin_budget_fte_amt                = case a_tbl_update(i).record_type
                                          when 'FTE_BUDGET_ACTUAL'
                                          then a_tbl_update(i).fin_budget_fte_amt
                                          else fin_budget_fte_amt end,
      fin_num_fte                       = case a_tbl_update(i).record_type
                                          when 'FTE_BUDGET_ACTUAL'
                                          then a_tbl_update(i).fin_num_fte
                                          else fin_num_fte end,
      float_discrepancy                 = case a_tbl_update(i).record_type
                                          when 'FLOAT_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).float_discrepancy
                                          else float_discrepancy end,
      float_holding                     = case a_tbl_update(i).record_type
                                          when 'FLOAT_HOLDING_ACTUAL'
                                          then a_tbl_update(i).float_holding
                                          else float_holding end,
      front_office_num_fte              = case a_tbl_update(i).record_type
                                          when 'FRONT_OFFICE_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).front_office_num_fte
                                          else front_office_num_fte end,
      gift_card_refunds                 = case a_tbl_update(i).record_type
                                          when 'GIFT_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).gift_card_refunds
                                          else gift_card_refunds end,
      gross_profit_budget               = case a_tbl_update(i).record_type
                                          when 'GROSS_PROFIT_BUDGET'
                                          then a_tbl_update(i).gross_profit_budget
                                          else gross_profit_budget end,
      hand_over_counts                  = case a_tbl_update(i).record_type
                                          when 'HAND_OVER_COUNTS_ACTUAL'
                                          then a_tbl_update(i).hand_over_counts
                                          else hand_over_counts end,
      independent_counts                = case a_tbl_update(i).record_type
                                          when 'INDEPENDENT_COUNTS_ACTUAL'
                                          then a_tbl_update(i).independent_counts
                                          else independent_counts end,
      net_margin_budget                 = case a_tbl_update(i).record_type
                                          when 'NET_MARGIN_BUDGET'
                                          then a_tbl_update(i).net_margin_budget
                                          else net_margin_budget end,
      non_cash_till_discrepancy         = case a_tbl_update(i).record_type
                                          when 'NON_CASH_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).non_cash_till_discrepancy
                                          else non_cash_till_discrepancy  end,
      num_tran_998_budget               = case a_tbl_update(i).record_type
                                          when 'CUSTOMERS_CHBFBUDGET'
                                          then a_tbl_update(i).num_tran_998_budget
                                          else num_tran_998_budget end,
      num_tran_999_budget               = case a_tbl_update(i).record_type
                                          when 'CUSTOMERS_BUDGET'
                                          then a_tbl_update(i).num_tran_999_budget
                                          else num_tran_999_budget end,
      num_tran_budget                   = case a_tbl_update(i).record_type
                                          when 'TRANSACTIONS_BUDGET'
                                          then a_tbl_update(i).num_tran_budget
                                          else num_tran_budget end,
      online_sales_budget               = case a_tbl_update(i).record_type
                                          when 'ONLINE_SALES_BUDGET'
                                          then a_tbl_update(i).online_sales_budget
                                          else online_sales_budget end,
      online_sales_budget_incl_vat      = case a_tbl_update(i).record_type
                                          when 'ONLINE_SALES_BUDGET'
                                          then a_tbl_update(i).online_sales_budget_incl_vat
                                          else online_sales_budget_incl_vat end,
      sales_margin_budget               = case a_tbl_update(i).record_type
                                          when 'SALES_MARGIN_BUDGET'
                                          then a_tbl_update(i).sales_margin_budget
                                          else sales_margin_budget end,
      sales_planned                     = case a_tbl_update(i).record_type
                                          when 'SALES_PLAN_BUDGET'
                                          then a_tbl_update(i).sales_planned
                                          else sales_planned end,
      sales_units_budget                = case a_tbl_update(i).record_type
                                          when 'SALES_UNITS_BUDGET'
                                          then a_tbl_update(i).sales_units_budget
                                          else sales_units_budget end,
      self_counts                       = case a_tbl_update(i).record_type
                                          when 'SELF_COUNTS_ACTUAL'
                                          then a_tbl_update(i).self_counts
                                          else self_counts end,
      shrinkage_cost_budget             = case a_tbl_update(i).record_type
                                          when 'SHRINKAGE_COST_BUDGET'
                                          then a_tbl_update(i).shrinkage_cost_budget
                                          else shrinkage_cost_budget end,
      voucher_till_discrepancy          = case a_tbl_update(i).record_type
                                          when 'VOUCHER_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).voucher_till_discrepancy
                                          else voucher_till_discrepancy end,
      waste_cost_budget                 = case a_tbl_update(i).record_type
                                          when 'WASTE_COST_BUDGET'
                                          then a_tbl_update(i).waste_cost_budget
                                          else waste_cost_budget end,
      waste_recovery_budget             = case a_tbl_update(i).record_type
                                          when 'WASTE_REC_BUDGET'
                                          then a_tbl_update(i).waste_recovery_budget
                                          else waste_recovery_budget end,
      waste_recovery_budget_incl_vat    = case a_tbl_update(i).record_type
                                          when 'WASTE_REC_BUDGET'
                                          then a_tbl_update(i).waste_recovery_budget_incl_vat
                                          else waste_recovery_budget_incl_vat end,
      ww_card_refunds                   = case a_tbl_update(i).record_type
                                          when 'WW_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).ww_card_refunds
                                          else ww_card_refunds end,
      wwcard_sales_budget_incl_vat      = case a_tbl_update(i).record_type
                                          when 'WWCARD_SALES_BUDGET'
                                          then a_tbl_update(i).wwcard_sales_budget_incl_vat
                                          else wwcard_sales_budget_incl_vat end,
      spec_dept_revenue_budget          = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_BUDGET'
                                          then a_tbl_update(i).spec_dept_revenue_budget
                                          else spec_dept_revenue_budget end,
      SPEC_DPT_REV_BUDGET_INCL_VAT      = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_BUDGET'
                                          then a_tbl_update(i).SPEC_DPT_REV_BUDGET_INCL_VAT
                                          else SPEC_DPT_REV_BUDGET_INCL_VAT end,          
                                          
      SPEC_DEPT_REVENUE_QTY_BUDGET      = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_QTY_BUDGET'
                                          then a_tbl_update(i).SPEC_DEPT_REVENUE_QTY_BUDGET
                                          else SPEC_DEPT_REVENUE_QTY_BUDGET end,
                                          
      SPEC_DPT_REV_QTY_BUDGT_INC_VAT    = case a_tbl_update(i).record_type
                                          when 'SPEC_DPT_REV_QTY_BUDGT_INC_VAT'
                                          then a_tbl_update(i).SPEC_DPT_REV_QTY_BUDGT_INC_VAT
                                          else SPEC_DPT_REV_QTY_BUDGT_INC_VAT end,
                                          
 --MULTICURRENCY ------



      sales_budget_mzar                 = case a_tbl_update(i).record_type
                                          when 'SALES_BUDGET'
                                          then a_tbl_update(i).sales_budget_mzar
                                          else sales_budget_mzar end ,
      back_office_num_fte_mzar          = case a_tbl_update(i).record_type
                                          when 'BACK_OFFICE_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).back_office_num_fte_mzar
                                          else back_office_num_fte_mzar end,
      bank_card_refunds_mzar            = case a_tbl_update(i).record_type
                                          when 'BANK_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).bank_card_refunds_mzar
                                          else bank_card_refunds_mzar end,
      cash_refunds_mzar                 = case a_tbl_update(i).record_type
                                          when 'CASH_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).cash_refunds_mzar
                                          else cash_refunds_mzar end,
      cash_till_discrepancy_mzar        = case a_tbl_update(i).record_type
                                          when 'CASH_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).cash_till_discrepancy_mzar
                                          else cash_till_discrepancy_mzar end,
      cheques_till_discrepancy_mzar     = case a_tbl_update(i).record_type
                                          when 'CHEQUES_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).cheques_till_discrepancy_mzar
                                          else cheques_till_discrepancy_mzar end,
      contractors_num_fte_mzar          = case a_tbl_update(i).record_type
                                          when 'CONTRACTORS_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).contractors_num_fte_mzar
                                          else contractors_num_fte_mzar end,
      credit_voucher_refunds_mzar       = case a_tbl_update(i).record_type
                                          when 'CREDIT_VOUCHER_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).credit_voucher_refunds_mzar
                                          else credit_voucher_refunds_mzar end,
      fin_budget_fte_amt_mzar           = case a_tbl_update(i).record_type
                                          when 'FTE_BUDGET_ACTUAL'
                                          then a_tbl_update(i).fin_budget_fte_amt_mzar
                                          else fin_budget_fte_amt_mzar end,
      fin_num_fte_mzar                 = case a_tbl_update(i).record_type
                                          when 'FTE_BUDGET_ACTUAL'
                                          then a_tbl_update(i).fin_num_fte_mzar
                                          else fin_num_fte_mzar end,
      float_discrepancy_mzar            = case a_tbl_update(i).record_type
                                          when 'FLOAT_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).float_discrepancy_mzar
                                          else float_discrepancy_mzar end,
      float_holding_mzar                = case a_tbl_update(i).record_type
                                          when 'FLOAT_HOLDING_ACTUAL'
                                          then a_tbl_update(i).float_holding_mzar
                                          else float_holding_mzar end,
      front_office_num_fte_mzar         = case a_tbl_update(i).record_type
                                          when 'FRONT_OFFICE_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).front_office_num_fte_mzar
                                          else front_office_num_fte_mzar end,
      gift_card_refunds_mzar                 = case a_tbl_update(i).record_type
                                          when 'GIFT_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).gift_card_refunds_mzar
                                          else gift_card_refunds_mzar end,
      gross_profit_budget_mzar          = case a_tbl_update(i).record_type
                                          when 'GROSS_PROFIT_BUDGET'
                                          then a_tbl_update(i).gross_profit_budget_mzar
                                          else gross_profit_budget_mzar end,
      hand_over_counts_mzar             = case a_tbl_update(i).record_type
                                          when 'HAND_OVER_COUNTS_ACTUAL'
                                          then a_tbl_update(i).hand_over_counts_mzar
                                          else hand_over_counts_mzar end,
      independent_counts_mzar           = case a_tbl_update(i).record_type
                                          when 'INDEPENDENT_COUNTS_ACTUAL'
                                          then a_tbl_update(i).independent_counts_mzar
                                          else independent_counts_mzar end,
      net_margin_budget_mzar           = case a_tbl_update(i).record_type
                                          when 'NET_MARGIN_BUDGET'
                                          then a_tbl_update(i).net_margin_budget_mzar
                                          else net_margin_budget_mzar end,
      non_cash_till_discrepancy_mzar   = case a_tbl_update(i).record_type
                                          when 'NON_CASH_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).non_cash_till_discrepancy_mzar
                                          else non_cash_till_discrepancy_mzar  end,
      num_tran_998_budget_mzar          = case a_tbl_update(i).record_type
                                          when 'CUSTOMERS_CHBFBUDGET'
                                          then a_tbl_update(i).num_tran_998_budget_mzar
                                          else num_tran_998_budget_mzar end,
      num_tran_999_budget_mzar          = case a_tbl_update(i).record_type
                                          when 'CUSTOMERS_BUDGET'
                                          then a_tbl_update(i).num_tran_999_budget_mzar
                                          else num_tran_999_budget_mzar end,
      num_tran_budget_mzar             = case a_tbl_update(i).record_type
                                          when 'TRANSACTIONS_BUDGET'
                                          then a_tbl_update(i).num_tran_budget_mzar
                                          else num_tran_budget_mzar end,
      online_sales_budget_mzar         = case a_tbl_update(i).record_type
                                          when 'ONLINE_SALES_BUDGET'
                                          then a_tbl_update(i).online_sales_budget_mzar
                                          else online_sales_budget_mzar end,
      online_sales_budget_ivat_mzar     = case a_tbl_update(i).record_type
                                          when 'ONLINE_SALES_BUDGET'
                                          then a_tbl_update(i).online_sales_budget_ivat_mzar
                                          else online_sales_budget_ivat_mzar end,
      sales_margin_budget_mzar          = case a_tbl_update(i).record_type
                                          when 'SALES_MARGIN_BUDGET'
                                          then a_tbl_update(i).sales_margin_budget_mzar
                                          else sales_margin_budget_mzar end,
      sales_planned_mzar                = case a_tbl_update(i).record_type
                                          when 'SALES_PLAN_BUDGET'
                                          then a_tbl_update(i).sales_planned_mzar
                                          else sales_planned_mzar end,
      sales_units_budget_mzar           = case a_tbl_update(i).record_type
                                          when 'SALES_UNITS_BUDGET'
                                          then a_tbl_update(i).sales_units_budget_mzar
                                          else sales_units_budget_mzar end,
      self_counts_mzar                  = case a_tbl_update(i).record_type
                                          when 'SELF_COUNTS_ACTUAL'
                                          then a_tbl_update(i).self_counts_mzar
                                          else self_counts_mzar end,
      shrinkage_cost_budget_mzar        = case a_tbl_update(i).record_type
                                          when 'SHRINKAGE_COST_BUDGET'
                                          then a_tbl_update(i).shrinkage_cost_budget_mzar
                                          else shrinkage_cost_budget_mzar end,
      voucher_till_discrepancy_mzar     = case a_tbl_update(i).record_type
                                          when 'VOUCHER_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).voucher_till_discrepancy_mzar
                                          else voucher_till_discrepancy_mzar end,
      waste_cost_budget_mzar            = case a_tbl_update(i).record_type
                                          when 'WASTE_COST_BUDGET'
                                          then a_tbl_update(i).waste_cost_budget_mzar
                                          else waste_cost_budget_mzar end,
      waste_recovery_budgt_mzar         = case a_tbl_update(i).record_type
                                          when 'WASTE_REC_BUDGET'
                                          then a_tbl_update(i).waste_recovery_budgt_mzar
                                          else waste_recovery_budgt_mzar end,
      ww_card_refunds_mzar              = case a_tbl_update(i).record_type
                                          when 'WW_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).ww_card_refunds_mzar
                                          else ww_card_refunds_mzar end,
      wwcard_sales_budget_ivat_mzar     = case a_tbl_update(i).record_type
                                          when 'WWCARD_SALES_BUDGET'
                                          then a_tbl_update(i).wwcard_sales_budget_ivat_mzar
                                          else wwcard_sales_budget_ivat_mzar end,
      spec_dept_revenue_budget_mzar     = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_BUDGET'
                                          then a_tbl_update(i).spec_dept_revenue_budget_mzar
                                          else spec_dept_revenue_budget_mzar end,
      SPEC_DPT_REV_BUDGET_IVAT_mzar     = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_BUDGET'
                                          then a_tbl_update(i).SPEC_DPT_REV_BUDGET_IVAT_MZAR
                                          else SPEC_DPT_REV_BUDGET_IVAT_MZAR end,          
      SPEC_DEPT_REVENUE_QTY_BDG_mzar    = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_QTY_BUDGET'
                                          then a_tbl_update(i).SPEC_DEPT_REVENUE_QTY_BDG_mzar
                                          else SPEC_DEPT_REVENUE_QTY_BDG_mzar end,
      SPEC_DPT_REV_QTY_BDG_IVAT_mzar    = case a_tbl_update(i).record_type
                                          when 'SPEC_DPT_REV_QTY_BUDGT_INC_VAT'
                                          then a_tbl_update(i).SPEC_DPT_REV_QTY_BDG_IVAT_mzar
                                          else SPEC_DPT_REV_QTY_BDG_IVAT_mzar end,
--Africa Local--                                         
      sales_budget_afr                 = case a_tbl_update(i).record_type
                                          when 'SALES_BUDGET'
                                          then a_tbl_update(i).sales_budget_afr
                                          else sales_budget_afr end ,
      back_office_num_fte_afr          = case a_tbl_update(i).record_type
                                          when 'BACK_OFFICE_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).back_office_num_fte_afr
                                          else back_office_num_fte_afr end,
      bank_card_refunds_afr            = case a_tbl_update(i).record_type
                                          when 'BANK_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).bank_card_refunds_afr
                                          else bank_card_refunds_afr end,
      cash_refunds_afr                 = case a_tbl_update(i).record_type
                                          when 'CASH_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).cash_refunds_afr
                                          else cash_refunds_afr end,
      cash_till_discrepancy_afr        = case a_tbl_update(i).record_type
                                          when 'CASH_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).cash_till_discrepancy_afr
                                          else cash_till_discrepancy_afr end,
      cheques_till_discrepancy_afr     = case a_tbl_update(i).record_type
                                          when 'CHEQUES_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).cheques_till_discrepancy_afr
                                          else cheques_till_discrepancy_afr end,
      contractors_num_fte_afr          = case a_tbl_update(i).record_type
                                          when 'CONTRACTORS_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).contractors_num_fte_afr
                                          else contractors_num_fte_afr end,
      credit_voucher_refunds_afr       = case a_tbl_update(i).record_type
                                          when 'CREDIT_VOUCHER_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).credit_voucher_refunds_afr
                                          else credit_voucher_refunds_afr end,
      fin_budget_fte_amt_afr           = case a_tbl_update(i).record_type
                                          when 'FTE_BUDGET_ACTUAL'
                                          then a_tbl_update(i).fin_budget_fte_amt_afr
                                          else fin_budget_fte_amt_afr end,
      fin_num_fte_afr                 = case a_tbl_update(i).record_type
                                          when 'FTE_BUDGET_ACTUAL'
                                          then a_tbl_update(i).fin_num_fte_afr
                                          else fin_num_fte_afr end,
      float_discrepancy_afr            = case a_tbl_update(i).record_type
                                          when 'FLOAT_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).float_discrepancy_afr
                                          else float_discrepancy_afr end,
      float_holding_afr                = case a_tbl_update(i).record_type
                                          when 'FLOAT_HOLDING_ACTUAL'
                                          then a_tbl_update(i).float_holding_afr
                                          else float_holding_afr end,
      front_office_num_fte_afr         = case a_tbl_update(i).record_type
                                          when 'FRONT_OFFICE_NUM_FTE_ACTUAL'
                                          then a_tbl_update(i).front_office_num_fte_afr
                                          else front_office_num_fte_afr end,
      gift_card_refunds_afr                 = case a_tbl_update(i).record_type
                                          when 'GIFT_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).gift_card_refunds_afr
                                          else gift_card_refunds_afr end,
      gross_profit_budget_afr          = case a_tbl_update(i).record_type
                                          when 'GROSS_PROFIT_BUDGET'
                                          then a_tbl_update(i).gross_profit_budget_afr
                                          else gross_profit_budget_afr end,
      hand_over_counts_afr             = case a_tbl_update(i).record_type
                                          when 'HAND_OVER_COUNTS_ACTUAL'
                                          then a_tbl_update(i).hand_over_counts_afr
                                          else hand_over_counts_afr end,
      independent_counts_afr           = case a_tbl_update(i).record_type
                                          when 'INDEPENDENT_COUNTS_ACTUAL'
                                          then a_tbl_update(i).independent_counts_afr
                                          else independent_counts_afr end,
      net_margin_budget_afr           = case a_tbl_update(i).record_type
                                          when 'NET_MARGIN_BUDGET'
                                          then a_tbl_update(i).net_margin_budget_afr
                                          else net_margin_budget_afr end,
      non_cash_till_discrepancy_afr   = case a_tbl_update(i).record_type
                                          when 'NON_CASH_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).non_cash_till_discrepancy_afr
                                          else non_cash_till_discrepancy_afr  end,
      num_tran_998_budget_afr          = case a_tbl_update(i).record_type
                                          when 'CUSTOMERS_CHBFBUDGET'
                                          then a_tbl_update(i).num_tran_998_budget_afr
                                          else num_tran_998_budget_afr end,
      num_tran_999_budget_afr          = case a_tbl_update(i).record_type
                                          when 'CUSTOMERS_BUDGET'
                                          then a_tbl_update(i).num_tran_999_budget_afr
                                          else num_tran_999_budget_afr end,
      num_tran_budget_afr             = case a_tbl_update(i).record_type
                                          when 'TRANSACTIONS_BUDGET'
                                          then a_tbl_update(i).num_tran_budget_afr
                                          else num_tran_budget_afr end,
      online_sales_budget_afr         = case a_tbl_update(i).record_type
                                          when 'ONLINE_SALES_BUDGET'
                                          then a_tbl_update(i).online_sales_budget_afr
                                          else online_sales_budget_afr end,
      online_sales_budget_ivat_afr     = case a_tbl_update(i).record_type
                                          when 'ONLINE_SALES_BUDGET'
                                          then a_tbl_update(i).online_sales_budget_ivat_afr
                                          else online_sales_budget_ivat_afr end,
      sales_margin_budget_afr          = case a_tbl_update(i).record_type
                                          when 'SALES_MARGIN_BUDGET'
                                          then a_tbl_update(i).sales_margin_budget_afr
                                          else sales_margin_budget_afr end,
      sales_planned_afr                = case a_tbl_update(i).record_type
                                          when 'SALES_PLAN_BUDGET'
                                          then a_tbl_update(i).sales_planned_afr
                                          else sales_planned_afr end,
      sales_units_budget_afr           = case a_tbl_update(i).record_type
                                          when 'SALES_UNITS_BUDGET'
                                          then a_tbl_update(i).sales_units_budget_afr
                                          else sales_units_budget_afr end,
      self_counts_afr                  = case a_tbl_update(i).record_type
                                          when 'SELF_COUNTS_ACTUAL'
                                          then a_tbl_update(i).self_counts_afr
                                          else self_counts_afr end,
      shrinkage_cost_budget_afr        = case a_tbl_update(i).record_type
                                          when 'SHRINKAGE_COST_BUDGET'
                                          then a_tbl_update(i).shrinkage_cost_budget_afr
                                          else shrinkage_cost_budget_afr end,
      voucher_till_discrepancy_afr     = case a_tbl_update(i).record_type
                                          when 'VOUCHER_TILL_DISCREPANCY_ACTUAL'
                                          then a_tbl_update(i).voucher_till_discrepancy_afr
                                          else voucher_till_discrepancy_afr end,
      waste_cost_budget_afr            = case a_tbl_update(i).record_type
                                          when 'WASTE_COST_BUDGET'
                                          then a_tbl_update(i).waste_cost_budget_afr
                                          else waste_cost_budget_afr end,
      waste_recovery_budgt_afr         = case a_tbl_update(i).record_type
                                          when 'WASTE_REC_BUDGET'
                                          then a_tbl_update(i).waste_recovery_budgt_afr
                                          else waste_recovery_budgt_afr end,
      ww_card_refunds_afr              = case a_tbl_update(i).record_type
                                          when 'WW_CARD_REFUNDS_ACTUAL'
                                          then a_tbl_update(i).ww_card_refunds_afr
                                          else ww_card_refunds_afr end,
      wwcard_sales_budget_ivat_afr     = case a_tbl_update(i).record_type
                                          when 'WWCARD_SALES_BUDGET'
                                          then a_tbl_update(i).wwcard_sales_budget_ivat_afr
                                          else wwcard_sales_budget_ivat_afr end,
      spec_dept_revenue_budget_afr     = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_BUDGET'
                                          then a_tbl_update(i).spec_dept_revenue_budget_afr
                                          else spec_dept_revenue_budget_afr end,
      SPEC_DPT_REV_BUDGET_IVAT_afr     = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_BUDGET'
                                          then a_tbl_update(i).SPEC_DPT_REV_BUDGET_IVAT_afr
                                          else SPEC_DPT_REV_BUDGET_IVAT_afr end,          
      SPEC_DEPT_REVENUE_QTY_BDG_afr    = case a_tbl_update(i).record_type
                                          when 'SPEC_DEPT_REVENUE_QTY_BUDGET'
                                          then a_tbl_update(i).SPEC_DEPT_REVENUE_QTY_BDG_afr
                                          else SPEC_DEPT_REVENUE_QTY_BDG_afr end,
      SPEC_DPT_REV_QTY_BDG_IVAT_afr    = case a_tbl_update(i).record_type
                                          when 'SPEC_DPT_REV_QTY_BUDGT_INC_VAT'
                                          then a_tbl_update(i).SPEC_DPT_REV_QTY_BDG_IVAT_afr
                                          else SPEC_DPT_REV_QTY_BDG_IVAT_afr end,
                                                                              
                                    
      source_data_status_code           = a_tbl_update(i).source_data_status_code,
      last_updated_date                 = a_tbl_update(i).last_updated_date
      where location_no           = a_tbl_update(i).location_no
      and department_no           = a_tbl_update(i).department_no
      and fin_year_no             = a_tbl_update(i).fin_year_no
      and fin_week_no             = a_tbl_update(i).fin_week_no;

g_recs_updated := g_recs_updated + a_tbl_update.count;

EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i) .error_index;
    l_message     := dwh_constants.vc_err_lb_update||i|| ' '||g_error_index|| ' '||sqlerrm( - sql%bulk_exceptions(i) .error_code) || ' '||a_tbl_update(g_error_index) .location_no|| ' '||a_tbl_update(g_error_index) .department_no|| ' '||a_tbl_update(g_error_index) .fin_year_no|| ' '||a_tbl_update(g_error_index) .fin_week_no;
    dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  END LOOP;
  raise;
END local_bulk_update;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_staging_update
AS
BEGIN

  forall i IN a_staging1.first .. a_staging1.last
  SAVE exceptions
   UPDATE stg_excel_fin_budg_cpy
  SET sys_process_code        = 'Y'
    WHERE sys_source_batch_id = a_staging1(i)
  AND sys_source_sequence_no  = a_staging2(i) ;
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i) .error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm( - sql%bulk_exceptions(i) .error_code) || ' '||a_staging1(g_error_index) ||' '||a_staging2(g_error_index) ;
    dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  END LOOP;
  raise;
END local_bulk_staging_update;
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
PROCEDURE local_write_output
AS
BEGIN
  g_found := FALSE;
  g_record_type           := g_rec_in.record_type;
  --DBMS_OUTPUT.PUT_LINE('LOCAL_WRITE -- REC='||G_REC_IN.RECORD_TYPE||' LOC='||g_rec_in.LOCATION_NO||' DEPT='||g_rec_in.DEPARTMENT_NO||' POSTDATE='||g_rec_in.POST_DATE||' EXCL='||G_REC_IN.FIELD_VALUE_EXCL_VAT ||' INCL='||G_REC_IN.FIELD_VALUE_INCL_VAT);

  -- Check to see if item is present on table and update/insert accordingly
   SELECT COUNT(1)
     INTO g_count
     FROM fnd_rtl_loc_dept_wk_fin_budg
    WHERE location_no = g_rec_out.location_no
  AND department_no   = g_rec_out.department_no
  AND fin_year_no     = g_rec_out.fin_year_no
  AND fin_week_no     = g_rec_out.fin_week_no;
  IF g_count          = 1 THEN
    g_found          := TRUE;
  END IF;

  -- Check if insert of item already in insert array and change to put duplicate in update array
  IF a_count_i > 0 AND NOT g_found THEN
    FOR i     IN a_tbl_insert.first .. a_tbl_insert.last
    LOOP
      IF a_tbl_insert(i) .location_no = g_rec_out.location_no
      AND a_tbl_insert(i) .department_no = g_rec_out.department_no
      AND a_tbl_insert(i) .fin_year_no = g_rec_out.fin_year_no
      AND a_tbl_insert(i) .fin_week_no = g_rec_out.fin_week_no
      THEN
        g_found                      := TRUE;
      END IF;
    END LOOP;
  END IF;
  -- Place data into and array for later writing to table in bulk
  IF NOT g_found THEN
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
--      dbms_output.put_line('ins - '||g_rec_out.LOCATION_NO
--||' ins - '||g_rec_out.DEPARTMENT_NO
--||' ins - '||g_rec_out.FIN_YEAR_NO
--||' ins - '||g_rec_out.FIN_WEEK_NO
--||' ins - '||g_rec_out.record_type
--);
  ELSE
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
--          dbms_output.put_line('upd - '||g_rec_out.LOCATION_NO
--||' upd - '||g_rec_out.DEPARTMENT_NO
--||' upd - '||g_rec_out.FIN_YEAR_NO
--||' upd - '||g_rec_out.FIN_WEEK_NO
--||' upd - '||g_rec_out.record_type
--);
  END IF;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- Bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  IF a_count > g_forall_limit THEN
    local_bulk_insert;
    --DBMS_OUTPUT.PUT_LINE('BEFORE UPDATE 1 -- '||G_REC_OUT.LOCATION_NO||'  '||G_REC_OUT.DEPARTMENT_NO||'  '||G_REC_OUT.FIN_YEAR_NO||G_REC_OUT.FIN_WEEK_NO);
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
  l_message := dwh_constants.vc_err_lw_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_lw_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  raise;
END local_write_output;
--**************************************************************************************************
-- Main process
--**************************************************************************************************
BEGIN
--DBMS_OUTPUT.ENABLE (1000000);
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit) ;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := 'LOAD ONLINE SALES BUDGET fnd_rtl_loc_dept_wk_fin_budg EX FINANCE SPREADSHEET STARTED AT '|| TO_CHAR(sysdate ,('dd mon yyyy hh24:mi:ss')) ;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  dwh_log.insert_log_summary(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_description , l_process_type ,dwh_constants.vc_log_started ,'' ,'' ,'' ,'' ,'') ;
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date) ;
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
    --**************************************************************************************************
  -- TRUNCATE temp_fnd_rtl_lc_dpt_wk_fn_bdg
  -- DWH_LOG.WRITE_LOG   : temp_fnd_rtl_lc_dpt_wk_fn_bdg is truncated before running
  --**************************************************************************************************
  execute immediate
  ('truncate table temp_fnd_rtl_lc_dpt_wk_fn_bdg');
  commit;
  l_text :='TRUNCATED temp_fnd_rtl_lc_dpt_wk_fn_bdg AT '||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  --**************************************************************************************************
  -- DELETE CURRENT YEAR FROM fnd_rtl_loc_dept_wk_fin_budg 
  -- FOR CURRENT FIN_YEAR AND ABOVE
  --**************************************************************************************************  
  DELETE /*+ PARALLEL(4) */ 
  FROM   fnd_rtl_loc_dept_wk_fin_budg
  WHERE  FIN_YEAR_NO >= (SELECT TODAY_FIN_YEAR_NO FROM DIM_CONTROL) 
  AND    (SELECT COUNT(*) FROM STG_EXCEL_FIN_BUDG_CPY WHERE SYS_PROCESS_CODE = 'N') > 1000   ;
 
  l_text :='DELETED CURRENT YEAR FORWARD FROM FIN_BUDGETS  '||sql%rowcount||'  '||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  COMMIT;
  
  --**************************************************************************************************
  -- Bulk fetch loop controlling main program execution
  --**************************************************************************************************
  OPEN c_stg_excel_loc_dept_wk;
  FETCH c_stg_excel_loc_dept_wk bulk collect
     INTO a_stg_input limit g_forall_limit;

  WHILE a_stg_input.count > 0
  LOOP
    FOR i IN 1 .. a_stg_input.count
    LOOP
      g_recs_read            := g_recs_read + 1;
      IF g_recs_read mod 50000 = 0 THEN
        l_text               := dwh_constants.vc_log_records_processed|| TO_CHAR(sysdate ,('dd mon yyyy hh24:mi:ss')) ||'  '||g_recs_read ;
        dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
      END IF;
      g_rec_in                := a_stg_input(i) ;
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
    FETCH c_stg_excel_loc_dept_wk bulk collect
       INTO a_stg_input limit g_forall_limit;
  END LOOP;
  CLOSE c_stg_excel_loc_dept_wk;
  --**************************************************************************************************
  -- At end write out what remains in the arrays at end of program
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
  local_bulk_staging_update;
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_description , l_process_type ,dwh_constants.vc_log_ended ,g_recs_read ,g_recs_inserted ,g_recs_updated ,'' ,g_recs_hospital) ;
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate ,('dd mon yyyy hh24:mi:ss')) ;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  l_text := ' ';
  dwh_log.write_log(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_text) ;
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  dwh_log.update_log_summary(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_description , l_process_type ,dwh_constants.vc_log_aborted ,'' ,'' ,'' ,'' ,'') ;
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name ,SQLCODE ,l_message) ;
  dwh_log.update_log_summary(l_name ,l_system_name ,l_script_name ,l_procedure_name ,l_description , l_process_type ,dwh_constants.vc_log_aborted ,'' ,'' ,'' ,'' ,'') ;
  ROLLBACK;
  p_success := false;
  raise;
END wh_fnd_corp_836u;
