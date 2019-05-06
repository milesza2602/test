--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_552U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_552U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Christie Koorts
--  Purpose:     Create location department week fact table in the performance layer with
--               added value ex foundation layer location department week finance budget table.
--  Tables:      Input  -   fnd_rtl_loc_dept_wk_fin_budg
--               Output -   rtl_loc_dept_wk
--  Packages:    constants, dwh_log, dwh_valid
--  Comments:    Single DML could be considered for this program.
--
--  Maintenance:
--  30 Jan 2009 - A Joshua  : TD-528 to include this_week_start_date
--  21 Feb 2009 - W Lyttle  : TD-861 analysis_svcs_sales_incl_vat, sales_adj 
--                                and sales_adj_incl_vat ca be removed 
--              - W Lyttle  : TD-861 sales_hl_ho_fcst can be removed 
--  1 April 2009 - defect 1223 - - no filter on driving cursor
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            rtl_loc_dept_wk%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_552U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE RTL_LOC_DEPT_WK EX FND_RTL_LOC_DEPT_WK_FIN_BUDG';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_dept_wk%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_dept_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_dept_wk_fin_budg is
   select ldwf.*,
          dl.sk1_location_no,
          dd.sk1_department_no,
          dlh.sk2_location_no,
          (sales_budget - sales_margin_budget) sales_cost_budget,
          (cash_refunds + ww_card_refunds + bank_card_refunds + credit_voucher_refunds + gift_card_refunds) till_refunds_total,
          (cash_till_discrepancy + cheques_till_discrepancy + voucher_till_discrepancy + non_cash_till_discrepancy + float_discrepancy) till_discrepancy_total,
          (independent_counts + hand_over_counts + self_counts) till_counts_total,
          fc.this_week_start_date, fc.fin_week_code
   from fnd_rtl_loc_dept_wk_fin_budg ldwf,
        dim_location dl,
        dim_department dd,
        dim_location_hist dlh,
        dim_calendar fc /* AJ - TD_528 changed from fnd_calendar to allow retrieval of this_week_start_date */
   where ldwf.location_no = dl.location_no
   and ldwf.department_no = dd.department_no
   and ldwf.location_no = dlh.location_no
   and ldwf.fin_year_no = fc.fin_year_no
   and ldwf.fin_week_no = fc.fin_week_no
   and fc.fin_day_no = 4
   and ldwf.last_updated_date = g_date
   and fc.calendar_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date;

-- Input record declared as cursor%rowtype
g_rec_in             c_fnd_rtl_loc_dept_wk_fin_budg%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_fnd_rtl_loc_dept_wk_fin_budg%rowtype;
a_stg_input      stg_array;

-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

--   where last_updated_date >= g_yesterday;
--   order by district_no

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_location_no                := g_rec_in.sk1_location_no;
   g_rec_out.sk1_department_no              := g_rec_in.sk1_department_no;
   g_rec_out.fin_year_no                    := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                    := g_rec_in.fin_week_no;
   g_rec_out.sk2_location_no                := g_rec_in.sk2_location_no;
 --  g_rec_out.analysis_svcs_sales_incl_vat   := g_rec_in.analysis_svcs_sales_incl_vat;
   g_rec_out.online_sales_budget            := g_rec_in.online_sales_budget;
   g_rec_out.online_sales_budget_incl_vat   := g_rec_in.online_sales_budget_incl_vat;
   g_rec_out.sales_budget                   := g_rec_in.sales_budget;
   g_rec_out.sales_budget_incl_vat          := g_rec_in.sales_budget_incl_vat;
   g_rec_out.waste_recovery_budget          := g_rec_in.waste_recovery_budget;
   g_rec_out.waste_recovery_budget_incl_vat := g_rec_in.waste_recovery_budget_incl_vat;
 --  g_rec_out.sales_hl_ho_fcst               := g_rec_in.sales_hl_ho_fcst;
--   g_rec_out.sales_adj                      := g_rec_in.sales_adj;
--   g_rec_out.sales_adj_incl_vat             := g_rec_in.sales_adj_incl_vat;
   g_rec_out.wwcard_sales_budget_incl_vat   := g_rec_in.wwcard_sales_budget_incl_vat;
   g_rec_out.fin_num_fte                    := g_rec_in.fin_num_fte;
   g_rec_out.fin_budget_fte_amt             := g_rec_in.fin_budget_fte_amt;
   g_rec_out.sales_units_budget             := g_rec_in.sales_units_budget;
   g_rec_out.sales_cost_budget              := g_rec_in.sales_cost_budget;
   g_rec_out.num_tran_budget                := g_rec_in.num_tran_budget;
   g_rec_out.num_tran_998_budget            := g_rec_in.num_tran_998_budget;
   g_rec_out.num_tran_999_budget            := g_rec_in.num_tran_999_budget;
   g_rec_out.waste_cost_budget              := g_rec_in.waste_cost_budget;
   g_rec_out.shrinkage_cost_budget          := g_rec_in.shrinkage_cost_budget;
   g_rec_out.sales_margin_budget            := g_rec_in.sales_margin_budget;
   g_rec_out.sales_planned                  := g_rec_in.sales_planned;
   g_rec_out.net_margin_budget              := g_rec_in.net_margin_budget;
   g_rec_out.gross_profit_budget            := g_rec_in.gross_profit_budget;
   g_rec_out.spec_dept_revenue_budget       := g_rec_in.spec_dept_revenue_budget;
   g_rec_out.spec_dpt_rev_budget_incl_vat   := g_rec_in.spec_dpt_rev_budget_incl_vat;
   g_rec_out.spec_dept_revenue_qty_budget   := g_rec_in.spec_dept_revenue_qty_budget;
   g_rec_out.spec_dpt_rev_qty_budgt_inc_vat := g_rec_in.spec_dpt_rev_qty_budgt_inc_vat;
   g_rec_out.front_office_num_fte           := g_rec_in.front_office_num_fte;
   g_rec_out.back_office_num_fte            := g_rec_in.back_office_num_fte;
   g_rec_out.contractors_num_fte            := g_rec_in.contractors_num_fte;
   g_rec_out.cash_refunds                   := g_rec_in.cash_refunds;
   g_rec_out.ww_card_refunds                := g_rec_in.ww_card_refunds;
   g_rec_out.bank_card_refunds              := g_rec_in.bank_card_refunds;
   g_rec_out.credit_voucher_refunds         := g_rec_in.credit_voucher_refunds;
   g_rec_out.gift_card_refunds              := g_rec_in.gift_card_refunds;
   g_rec_out.till_refunds_total             := g_rec_in.till_refunds_total;
   g_rec_out.cash_till_discrepancy          := g_rec_in.cash_till_discrepancy;
   g_rec_out.cheques_till_discrepancy       := g_rec_in.cheques_till_discrepancy;
   g_rec_out.voucher_till_discrepancy       := g_rec_in.voucher_till_discrepancy;
   g_rec_out.non_cash_till_discrepancy      := g_rec_in.non_cash_till_discrepancy;
   g_rec_out.float_discrepancy              := g_rec_in.float_discrepancy;
   g_rec_out.till_discrepancy_total         := g_rec_in.till_discrepancy_total;
   g_rec_out.float_holding                  := g_rec_in.float_holding;
   g_rec_out.independent_counts             := g_rec_in.independent_counts;
   g_rec_out.hand_over_counts               := g_rec_in.hand_over_counts;
   g_rec_out.self_counts                    := g_rec_in.self_counts;
   g_rec_out.till_counts_total              := g_rec_in.till_counts_total;
   g_rec_out.this_week_start_date           := g_rec_in.this_week_start_date;
   g_rec_out.fin_week_code                  := g_rec_in.fin_week_code;
   g_rec_out.last_updated_date              := g_date;
   
--MC--
      g_rec_out.online_sales_budget_mzar        := g_rec_in.online_sales_budget_mzar;
      g_rec_out.online_sales_budget_ivat_mzar   := g_rec_in.online_sales_budget_ivat_mzar;
      g_rec_out.sales_budget_mzar               := g_rec_in.sales_budget_mzar;
      g_rec_out.sales_budget_incl_vat_mzar      := g_rec_in.sales_budget_incl_vat_mzar;
      g_rec_out.waste_recovery_budgt_mzar       := g_rec_in.waste_recovery_budgt_mzar;
      g_rec_out.waste_recovery_budgt_ivat_mzar  := g_rec_in.waste_recovery_budgt_ivat_mzar;
      g_rec_out.wwcard_sales_budget_ivat_mzar   := g_rec_in.wwcard_sales_budget_ivat_mzar;
      g_rec_out.fin_num_fte_mzar                := g_rec_in.fin_num_fte_mzar;
      g_rec_out.fin_budget_fte_amt_mzar         := g_rec_in.fin_budget_fte_amt_mzar;
      g_rec_out.sales_units_budget_mzar         := g_rec_in.sales_units_budget_mzar;
      g_rec_out.num_tran_budget_mzar            := g_rec_in.num_tran_budget_mzar;
      g_rec_out.num_tran_998_budget_mzar        := g_rec_in.num_tran_998_budget_mzar;
      g_rec_out.num_tran_999_budget_mzar        := g_rec_in.num_tran_999_budget_mzar;
      g_rec_out.waste_cost_budget_mzar          := g_rec_in.waste_cost_budget_mzar;
      g_rec_out.shrinkage_cost_budget_mzar      := g_rec_in.shrinkage_cost_budget_mzar;
      g_rec_out.sales_margin_budget_mzar        := g_rec_in.sales_margin_budget_mzar;
      g_rec_out.sales_planned_mzar              := g_rec_in.sales_planned_mzar;
      g_rec_out.net_margin_budget_mzar          := g_rec_in.net_margin_budget_mzar;
      g_rec_out.gross_profit_budget_mzar        := g_rec_in.gross_profit_budget_mzar;
      g_rec_out.front_office_num_fte_mzar       := g_rec_in.front_office_num_fte_mzar;
      g_rec_out.back_office_num_fte_mzar        := g_rec_in.back_office_num_fte_mzar;
      g_rec_out.contractors_num_fte_mzar        := g_rec_in.contractors_num_fte_mzar;
      g_rec_out.cash_refunds_mzar               := g_rec_in.cash_refunds_mzar;
      g_rec_out.ww_card_refunds_mzar            := g_rec_in.ww_card_refunds_mzar;
      g_rec_out.bank_card_refunds_mzar          := g_rec_in.bank_card_refunds_mzar;
      g_rec_out.credit_voucher_refunds_mzar     := g_rec_in.credit_voucher_refunds_mzar;
      g_rec_out.gift_card_refunds_mzar          := g_rec_in.gift_card_refunds_mzar;
      g_rec_out.cash_till_discrepancy_mzar      := g_rec_in.cash_till_discrepancy_mzar;
      g_rec_out.cheques_till_discrepancy_mzar   := g_rec_in.cheques_till_discrepancy_mzar;
      g_rec_out.voucher_till_discrepancy_mzar   := g_rec_in.voucher_till_discrepancy_mzar;
      g_rec_out.non_cash_till_discrepancy_mzar  := g_rec_in.non_cash_till_discrepancy_mzar;
      g_rec_out.float_discrepancy_mzar          := g_rec_in.float_discrepancy_mzar;
      g_rec_out.float_holding_mzar              := g_rec_in.float_holding_mzar;
      g_rec_out.independent_counts_mzar         := g_rec_in.independent_counts_mzar;
      g_rec_out.hand_over_counts_mzar           := g_rec_in.hand_over_counts_mzar;
      g_rec_out.self_counts_mzar                := g_rec_in.self_counts_mzar;
      g_rec_out.spec_dept_revenue_budget_mzar   := g_rec_in.spec_dept_revenue_budget_mzar;
      g_rec_out.spec_dpt_rev_budget_ivat_mzar   := g_rec_in.spec_dpt_rev_budget_ivat_mzar;
      g_rec_out.spec_dept_revenue_qty_bdg_mzar  := g_rec_in.spec_dept_revenue_qty_bdg_mzar;
      g_rec_out.spec_dpt_rev_qty_bdg_ivat_mzar  := g_rec_in.spec_dpt_rev_qty_bdg_ivat_mzar;
      g_rec_out.online_sales_budget_afr         := g_rec_in.online_sales_budget_afr;
      g_rec_out.online_sales_budget_ivat_afr    := g_rec_in.online_sales_budget_ivat_afr;
      g_rec_out.sales_budget_afr                := g_rec_in.sales_budget_afr;
      g_rec_out.sales_budget_incl_vat_afr       := g_rec_in.sales_budget_incl_vat_afr;
      g_rec_out.waste_recovery_budgt_afr        := g_rec_in.waste_recovery_budgt_afr;
      g_rec_out.waste_recovery_budgt_ivat_afr   := g_rec_in.waste_recovery_budgt_ivat_afr;
      g_rec_out.wwcard_sales_budget_ivat_afr    := g_rec_in.wwcard_sales_budget_ivat_afr;
      g_rec_out.fin_num_fte_afr                 := g_rec_in.fin_num_fte_afr;
      g_rec_out.fin_budget_fte_amt_afr          := g_rec_in.fin_budget_fte_amt_afr;
      g_rec_out.sales_units_budget_afr          := g_rec_in.sales_units_budget_afr;
      g_rec_out.num_tran_budget_afr             := g_rec_in.num_tran_budget_afr;
      g_rec_out.num_tran_998_budget_afr         := g_rec_in.num_tran_998_budget_afr;
      g_rec_out.num_tran_999_budget_afr         := g_rec_in.num_tran_999_budget_afr;
      g_rec_out.waste_cost_budget_afr           := g_rec_in.waste_cost_budget_afr;
      g_rec_out.shrinkage_cost_budget_afr       := g_rec_in.shrinkage_cost_budget_afr;
      g_rec_out.sales_margin_budget_afr         := g_rec_in.sales_margin_budget_afr;
      g_rec_out.sales_planned_afr               := g_rec_in.sales_planned_afr;
      g_rec_out.net_margin_budget_afr           := g_rec_in.net_margin_budget_afr;
      g_rec_out.gross_profit_budget_afr         := g_rec_in.gross_profit_budget_afr;
      g_rec_out.front_office_num_fte_afr        := g_rec_in.front_office_num_fte_afr;
      g_rec_out.back_office_num_fte_afr         := g_rec_in.back_office_num_fte_afr;
      g_rec_out.contractors_num_fte_afr         := g_rec_in.contractors_num_fte_afr;
      g_rec_out.cash_refunds_afr                := g_rec_in.cash_refunds_afr;
      g_rec_out.ww_card_refunds_afr             := g_rec_in.ww_card_refunds_afr;
      g_rec_out.bank_card_refunds_afr           := g_rec_in.bank_card_refunds_afr;
      g_rec_out.credit_voucher_refunds_afr      := g_rec_in.credit_voucher_refunds_afr;
      g_rec_out.gift_card_refunds_afr           := g_rec_in.gift_card_refunds_afr;
      g_rec_out.cash_till_discrepancy_afr       := g_rec_in.cash_till_discrepancy_afr;
      g_rec_out.cheques_till_discrepancy_afr    := g_rec_in.cheques_till_discrepancy_afr;
      g_rec_out.voucher_till_discrepancy_afr    := g_rec_in.voucher_till_discrepancy_afr;
      g_rec_out.non_cash_till_discrepancy_afr   := g_rec_in.non_cash_till_discrepancy_afr;
      g_rec_out.float_discrepancy_afr           := g_rec_in.float_discrepancy_afr;
      g_rec_out.float_holding_afr               := g_rec_in.float_holding_afr;
      g_rec_out.independent_counts_afr          := g_rec_in.independent_counts_afr;
      g_rec_out.hand_over_counts_afr            := g_rec_in.hand_over_counts_afr;
      g_rec_out.self_counts_afr                 := g_rec_in.self_counts_afr;
      g_rec_out.spec_dept_revenue_budget_afr    := g_rec_in.spec_dept_revenue_budget_afr;
      g_rec_out.spec_dpt_rev_budget_ivat_afr    := g_rec_in.spec_dpt_rev_budget_ivat_afr;
      g_rec_out.spec_dept_revenue_qty_bdg_afr   := g_rec_in.spec_dept_revenue_qty_bdg_afr;
      g_rec_out.spec_dpt_rev_qty_bdg_ivat_afr   := g_rec_in.spec_dpt_rev_qty_bdg_ivat_afr;
   

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into rtl_loc_dept_wk values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_department_no||
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no;
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
      update rtl_loc_dept_wk
      set sk1_location_no                = a_tbl_update(i).sk1_location_no,
          sk1_department_no              = a_tbl_update(i).sk1_department_no,
          fin_year_no                    = a_tbl_update(i).fin_year_no,
          fin_week_no                    = a_tbl_update(i).fin_week_no,
          sk2_location_no                = a_tbl_update(i).sk2_location_no,
--          analysis_svcs_sales_incl_vat   = a_tbl_update(i).analysis_svcs_sales_incl_vat,
          online_sales_budget            = a_tbl_update(i).online_sales_budget,
          online_sales_budget_incl_vat   = a_tbl_update(i).online_sales_budget_incl_vat,
          sales_budget                   = a_tbl_update(i).sales_budget,
          sales_budget_incl_vat          = a_tbl_update(i).sales_budget_incl_vat,
          waste_recovery_budget          = a_tbl_update(i).waste_recovery_budget,
          waste_recovery_budget_incl_vat = a_tbl_update(i).waste_recovery_budget_incl_vat,
 --         sales_hl_ho_fcst               = a_tbl_update(i).sales_hl_ho_fcst,
 --         sales_adj                      = a_tbl_update(i).sales_adj,
 --         sales_adj_incl_vat             = a_tbl_update(i).sales_adj_incl_vat,
          wwcard_sales_budget_incl_vat   = a_tbl_update(i).wwcard_sales_budget_incl_vat,
          fin_num_fte                    = a_tbl_update(i).fin_num_fte,
          fin_budget_fte_amt             = a_tbl_update(i).fin_budget_fte_amt,
          sales_units_budget             = a_tbl_update(i).sales_units_budget,
          sales_cost_budget              = a_tbl_update(i).sales_cost_budget,
          num_tran_budget                = a_tbl_update(i).num_tran_budget,
          num_tran_998_budget            = a_tbl_update(i).num_tran_998_budget,
          num_tran_999_budget            = a_tbl_update(i).num_tran_999_budget,
          waste_cost_budget              = a_tbl_update(i).waste_cost_budget,
          shrinkage_cost_budget          = a_tbl_update(i).shrinkage_cost_budget,
          sales_margin_budget            = a_tbl_update(i).sales_margin_budget,
          spec_dept_revenue_budget       = a_tbl_update(i).spec_dept_revenue_budget,
          spec_dpt_rev_budget_incl_vat   = a_tbl_update(i).spec_dpt_rev_budget_incl_vat,
          spec_dept_revenue_qty_budget   = a_tbl_update(i).spec_dept_revenue_qty_budget,
          spec_dpt_rev_qty_budgt_inc_vat = a_tbl_update(i).spec_dpt_rev_qty_budgt_inc_vat,
          sales_planned                  = a_tbl_update(i).sales_planned,
          net_margin_budget              = a_tbl_update(i).net_margin_budget,
          gross_profit_budget            = a_tbl_update(i).gross_profit_budget,
          front_office_num_fte           = a_tbl_update(i).front_office_num_fte,
          back_office_num_fte            = a_tbl_update(i).back_office_num_fte,
          contractors_num_fte            = a_tbl_update(i).contractors_num_fte,
          cash_refunds                   = a_tbl_update(i).cash_refunds,
          ww_card_refunds                = a_tbl_update(i).ww_card_refunds,
          bank_card_refunds              = a_tbl_update(i).bank_card_refunds,
          credit_voucher_refunds         = a_tbl_update(i).credit_voucher_refunds,
          gift_card_refunds              = a_tbl_update(i).gift_card_refunds,
          till_refunds_total             = a_tbl_update(i).till_refunds_total,
          cash_till_discrepancy          = a_tbl_update(i).cash_till_discrepancy,
          cheques_till_discrepancy       = a_tbl_update(i).cheques_till_discrepancy,
          voucher_till_discrepancy       = a_tbl_update(i).voucher_till_discrepancy,
          non_cash_till_discrepancy      = a_tbl_update(i).non_cash_till_discrepancy,
          float_discrepancy              = a_tbl_update(i).float_discrepancy,
          till_discrepancy_total         = a_tbl_update(i).till_discrepancy_total,
          float_holding                  = a_tbl_update(i).float_holding,
          independent_counts             = a_tbl_update(i).independent_counts,
          hand_over_counts               = a_tbl_update(i).hand_over_counts,
          self_counts                    = a_tbl_update(i).self_counts,
          till_counts_total              = a_tbl_update(i).till_counts_total,
          this_week_start_date           = a_tbl_update(i).this_week_start_date,
          fin_week_code                  = a_tbl_update(i).fin_week_code,
--MC--          
          online_sales_budget_mzar        = a_tbl_update(i).online_sales_budget_mzar,
          online_sales_budget_ivat_mzar   = a_tbl_update(i).online_sales_budget_ivat_mzar,
          sales_budget_mzar               = a_tbl_update(i).sales_budget_mzar,
          sales_budget_incl_vat_mzar      = a_tbl_update(i).sales_budget_incl_vat_mzar,
          waste_recovery_budgt_mzar       = a_tbl_update(i).waste_recovery_budgt_mzar,
          waste_recovery_budgt_ivat_mzar  = a_tbl_update(i).waste_recovery_budgt_ivat_mzar,
          wwcard_sales_budget_ivat_mzar   = a_tbl_update(i).wwcard_sales_budget_ivat_mzar,
          fin_num_fte_mzar                = a_tbl_update(i).fin_num_fte_mzar,
          fin_budget_fte_amt_mzar         = a_tbl_update(i).fin_budget_fte_amt_mzar,
          sales_units_budget_mzar         = a_tbl_update(i).sales_units_budget_mzar,
          num_tran_budget_mzar            = a_tbl_update(i).num_tran_budget_mzar,
          num_tran_998_budget_mzar        = a_tbl_update(i).num_tran_998_budget_mzar,
          num_tran_999_budget_mzar        = a_tbl_update(i).num_tran_999_budget_mzar,
          waste_cost_budget_mzar          = a_tbl_update(i).waste_cost_budget_mzar,
          shrinkage_cost_budget_mzar      = a_tbl_update(i).shrinkage_cost_budget_mzar,
          sales_margin_budget_mzar        = a_tbl_update(i).sales_margin_budget_mzar,
          sales_planned_mzar              = a_tbl_update(i).sales_planned_mzar,
          net_margin_budget_mzar          = a_tbl_update(i).net_margin_budget_mzar,
          gross_profit_budget_mzar        = a_tbl_update(i).gross_profit_budget_mzar,
          front_office_num_fte_mzar       = a_tbl_update(i).front_office_num_fte_mzar,
          back_office_num_fte_mzar        = a_tbl_update(i).back_office_num_fte_mzar,
          contractors_num_fte_mzar        = a_tbl_update(i).contractors_num_fte_mzar,
          cash_refunds_mzar               = a_tbl_update(i).cash_refunds_mzar,
          ww_card_refunds_mzar            = a_tbl_update(i).ww_card_refunds_mzar,
          bank_card_refunds_mzar          = a_tbl_update(i).bank_card_refunds_mzar,
          credit_voucher_refunds_mzar     = a_tbl_update(i).credit_voucher_refunds_mzar,
          gift_card_refunds_mzar          = a_tbl_update(i).gift_card_refunds_mzar,
          cash_till_discrepancy_mzar      = a_tbl_update(i).cash_till_discrepancy_mzar,
          cheques_till_discrepancy_mzar   = a_tbl_update(i).cheques_till_discrepancy_mzar,
          voucher_till_discrepancy_mzar   = a_tbl_update(i).voucher_till_discrepancy_mzar,
          non_cash_till_discrepancy_mzar  = a_tbl_update(i).non_cash_till_discrepancy_mzar,
          float_discrepancy_mzar          = a_tbl_update(i).float_discrepancy_mzar,
          float_holding_mzar              = a_tbl_update(i).float_holding_mzar,
          independent_counts_mzar         = a_tbl_update(i).independent_counts_mzar,
          hand_over_counts_mzar           = a_tbl_update(i).hand_over_counts_mzar,
          self_counts_mzar                = a_tbl_update(i).self_counts_mzar,
          spec_dept_revenue_budget_mzar   = a_tbl_update(i).spec_dept_revenue_budget_mzar,
          spec_dpt_rev_budget_ivat_mzar   = a_tbl_update(i).spec_dpt_rev_budget_ivat_mzar,
          spec_dept_revenue_qty_bdg_mzar  = a_tbl_update(i).spec_dept_revenue_qty_bdg_mzar,
          spec_dpt_rev_qty_bdg_ivat_mzar  = a_tbl_update(i).spec_dpt_rev_qty_bdg_ivat_mzar,
          online_sales_budget_afr         = a_tbl_update(i).online_sales_budget_afr,
          online_sales_budget_ivat_afr    = a_tbl_update(i).online_sales_budget_ivat_afr,
          sales_budget_afr                = a_tbl_update(i).sales_budget_afr,
          sales_budget_incl_vat_afr       = a_tbl_update(i).sales_budget_incl_vat_afr,
          waste_recovery_budgt_afr        = a_tbl_update(i).waste_recovery_budgt_afr,
          waste_recovery_budgt_ivat_afr   = a_tbl_update(i).waste_recovery_budgt_ivat_afr,
          wwcard_sales_budget_ivat_afr    = a_tbl_update(i).wwcard_sales_budget_ivat_afr,
          fin_num_fte_afr                 = a_tbl_update(i).fin_num_fte_afr,
          fin_budget_fte_amt_afr          = a_tbl_update(i).fin_budget_fte_amt_afr,
          sales_units_budget_afr          = a_tbl_update(i).sales_units_budget_afr,
          num_tran_budget_afr             = a_tbl_update(i).num_tran_budget_afr,
          num_tran_998_budget_afr         = a_tbl_update(i).num_tran_998_budget_afr,
          num_tran_999_budget_afr         = a_tbl_update(i).num_tran_999_budget_afr,
          waste_cost_budget_afr           = a_tbl_update(i).waste_cost_budget_afr,
          shrinkage_cost_budget_afr       = a_tbl_update(i).shrinkage_cost_budget_afr,
          sales_margin_budget_afr         = a_tbl_update(i).sales_margin_budget_afr,
          sales_planned_afr               = a_tbl_update(i).sales_planned_afr,
          net_margin_budget_afr           = a_tbl_update(i).net_margin_budget_afr,
          gross_profit_budget_afr         = a_tbl_update(i).gross_profit_budget_afr,
          front_office_num_fte_afr        = a_tbl_update(i).front_office_num_fte_afr,
          back_office_num_fte_afr         = a_tbl_update(i).back_office_num_fte_afr,
          contractors_num_fte_afr         = a_tbl_update(i).contractors_num_fte_afr,
          cash_refunds_afr                = a_tbl_update(i).cash_refunds_afr,
          ww_card_refunds_afr             = a_tbl_update(i).ww_card_refunds_afr,
          bank_card_refunds_afr           = a_tbl_update(i).bank_card_refunds_afr,
          credit_voucher_refunds_afr      = a_tbl_update(i).credit_voucher_refunds_afr,
          gift_card_refunds_afr           = a_tbl_update(i).gift_card_refunds_afr,
          cash_till_discrepancy_afr       = a_tbl_update(i).cash_till_discrepancy_afr,
          cheques_till_discrepancy_afr    = a_tbl_update(i).cheques_till_discrepancy_afr,
          voucher_till_discrepancy_afr    = a_tbl_update(i).voucher_till_discrepancy_afr,
          non_cash_till_discrepancy_afr   = a_tbl_update(i).non_cash_till_discrepancy_afr,
          float_discrepancy_afr           = a_tbl_update(i).float_discrepancy_afr,
          float_holding_afr               = a_tbl_update(i).float_holding_afr,
          independent_counts_afr          = a_tbl_update(i).independent_counts_afr,
          hand_over_counts_afr            = a_tbl_update(i).hand_over_counts_afr,
          self_counts_afr                 = a_tbl_update(i).self_counts_afr,
          spec_dept_revenue_budget_afr    = a_tbl_update(i).spec_dept_revenue_budget_afr,
          spec_dpt_rev_budget_ivat_afr    = a_tbl_update(i).spec_dpt_rev_budget_ivat_afr,
          spec_dept_revenue_qty_bdg_afr   = a_tbl_update(i).spec_dept_revenue_qty_bdg_afr,
          spec_dpt_rev_qty_bdg_ivat_afr   = a_tbl_update(i).spec_dpt_rev_qty_bdg_ivat_afr,
          
          last_updated_date              = a_tbl_update(i).last_updated_date
      where sk1_location_no = a_tbl_update(i).sk1_location_no
      and   sk1_department_no = a_tbl_update(i).sk1_department_no
      and   fin_year_no = a_tbl_update(i).fin_year_no
      and   fin_week_no = a_tbl_update(i).fin_week_no;

      g_recs_updated := g_recs_updated + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_department_no||
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
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
   select count(1)
   into g_count
   from rtl_loc_dept_wk
   where sk1_location_no = g_rec_out.sk1_location_no
   and   sk1_department_no = g_rec_out.sk1_department_no
   and   fin_year_no = g_rec_out.fin_year_no
   and   fin_week_no = g_rec_out.fin_week_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

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
-- Main process loop
--**************************************************************************************************
begin

    dbms_output.put_line('Creating data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LOC_DEPT_WK EX FND_RTL_LOC_DEPT_WK_FIN_BUDG STARTED AT '||
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
    
  --**************************************************************************************************
  -- CLEAR CURRENT YEAR FROM rtl_loc_dept_wk
  -- FOR CURRENT FIN_YEAR AND ABOVE
  --**************************************************************************************************  
  l_text :='CLEAR CURRENT YEAR FORWARD FIN_BUDGETS AT '||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

  UPDATE /*+ PARALLEL(ldw,8) */ rtl_loc_dept_wk ldw
  SET     online_sales_budget            = 0,
          online_sales_budget_incl_vat   = 0,
          sales_budget                   = 0,
          sales_budget_incl_vat          = 0,
          waste_recovery_budget          = 0,
          waste_recovery_budget_incl_vat = 0,
          wwcard_sales_budget_incl_vat   = 0,
          fin_num_fte                    = 0,
          fin_budget_fte_amt             = 0,
          sales_units_budget             = 0,
          sales_cost_budget              = 0,
          num_tran_budget                = 0,
          num_tran_998_budget            = 0,
          num_tran_999_budget            = 0,
          waste_cost_budget              = 0,
          shrinkage_cost_budget          = 0,
          sales_margin_budget            = 0,
          spec_dept_revenue_budget       = 0,
          spec_dpt_rev_budget_incl_vat   = 0,
          spec_dept_revenue_qty_budget   = 0,
          spec_dpt_rev_qty_budgt_inc_vat = 0,
          sales_planned                  = 0,
          net_margin_budget              = 0,
          gross_profit_budget            = 0,
          front_office_num_fte           = 0,
          back_office_num_fte            = 0,
          contractors_num_fte            = 0,
          cash_refunds                   = 0,
          ww_card_refunds                = 0,
          bank_card_refunds              = 0,
          credit_voucher_refunds         = 0,
          gift_card_refunds              = 0,
          till_refunds_total             = 0,
          cash_till_discrepancy          = 0,
          cheques_till_discrepancy       = 0,
          voucher_till_discrepancy       = 0,
          non_cash_till_discrepancy      = 0,
          float_discrepancy              = 0,
          till_discrepancy_total         = 0,
          float_holding                  = 0,
          independent_counts             = 0,
          hand_over_counts               = 0,
          self_counts                    = 0,
          till_counts_total              = 0, 
--MC--          
          ONLINE_SALES_BUDGET_MZAR	=  0,
          ONLINE_SALES_BUDGET_IVAT_MZAR	=  0,
          SALES_BUDGET_MZAR	=  0,
          SALES_BUDGET_INCL_VAT_MZAR	=  0,
          WASTE_RECOVERY_BUDGT_MZAR	=  0,
          WASTE_RECOVERY_BUDGT_IVAT_MZAR	=  0,
          WWCARD_SALES_BUDGET_IVAT_MZAR	=  0,
          FIN_NUM_FTE_MZAR	=  0,
          FIN_BUDGET_FTE_AMT_MZAR	=  0,
          SALES_UNITS_BUDGET_MZAR	=  0,
          NUM_TRAN_BUDGET_MZAR	=  0,
          NUM_TRAN_998_BUDGET_MZAR	=  0,
          NUM_TRAN_999_BUDGET_MZAR	=  0,
          WASTE_COST_BUDGET_MZAR	=  0,
          SHRINKAGE_COST_BUDGET_MZAR	=  0,
          SALES_MARGIN_BUDGET_MZAR	=  0,
          SALES_PLANNED_MZAR	=  0,
          NET_MARGIN_BUDGET_MZAR	=  0,
          GROSS_PROFIT_BUDGET_MZAR	=  0,
          FRONT_OFFICE_NUM_FTE_MZAR	=  0,
          BACK_OFFICE_NUM_FTE_MZAR	=  0,
          CONTRACTORS_NUM_FTE_MZAR	=  0,
          CASH_REFUNDS_MZAR	=  0,
          WW_CARD_REFUNDS_MZAR	=  0,
          BANK_CARD_REFUNDS_MZAR	=  0,
          CREDIT_VOUCHER_REFUNDS_MZAR	=  0,
          GIFT_CARD_REFUNDS_MZAR	=  0,
          CASH_TILL_DISCREPANCY_MZAR	=  0,
          CHEQUES_TILL_DISCREPANCY_MZAR	=  0,
          VOUCHER_TILL_DISCREPANCY_MZAR	=  0,
          NON_CASH_TILL_DISCREPANCY_MZAR	=  0,
          FLOAT_DISCREPANCY_MZAR	=  0,
          FLOAT_HOLDING_MZAR	=  0,
          INDEPENDENT_COUNTS_MZAR	=  0,
          HAND_OVER_COUNTS_MZAR	=  0,
          SELF_COUNTS_MZAR	=  0,
          SPEC_DEPT_REVENUE_BUDGET_MZAR	=  0,
          SPEC_DPT_REV_BUDGET_IVAT_MZAR	=  0,
          SPEC_DEPT_REVENUE_QTY_BDG_MZAR	=  0,
          SPEC_DPT_REV_QTY_BDG_IVAT_MZAR	=  0,
          ONLINE_SALES_BUDGET_AFR	=  0,
          ONLINE_SALES_BUDGET_IVAT_AFR	=  0,
          SALES_BUDGET_AFR	=  0,
          SALES_BUDGET_INCL_VAT_AFR	=  0,
          WASTE_RECOVERY_BUDGT_AFR	=  0,
          WASTE_RECOVERY_BUDGT_IVAT_AFR	=  0,
          WWCARD_SALES_BUDGET_IVAT_AFR	=  0,
          FIN_NUM_FTE_AFR	=  0,
          FIN_BUDGET_FTE_AMT_AFR	=  0,
          SALES_UNITS_BUDGET_AFR	=  0,
          NUM_TRAN_BUDGET_AFR	=  0,
          NUM_TRAN_998_BUDGET_AFR	=  0,
          NUM_TRAN_999_BUDGET_AFR	=  0,
          WASTE_COST_BUDGET_AFR	=  0,
          SHRINKAGE_COST_BUDGET_AFR	=  0,
          SALES_MARGIN_BUDGET_AFR	=  0,
          SALES_PLANNED_AFR	=  0,
          NET_MARGIN_BUDGET_AFR	=  0,
          GROSS_PROFIT_BUDGET_AFR	=  0,
          FRONT_OFFICE_NUM_FTE_AFR	=  0,
          BACK_OFFICE_NUM_FTE_AFR	=  0,
          CONTRACTORS_NUM_FTE_AFR	=  0,
          CASH_REFUNDS_AFR	=  0,
          WW_CARD_REFUNDS_AFR	=  0,
          BANK_CARD_REFUNDS_AFR	=  0,
          CREDIT_VOUCHER_REFUNDS_AFR	=  0,
          GIFT_CARD_REFUNDS_AFR	=  0,
          CASH_TILL_DISCREPANCY_AFR	=  0,
          CHEQUES_TILL_DISCREPANCY_AFR	=  0,
          VOUCHER_TILL_DISCREPANCY_AFR	=  0,
          NON_CASH_TILL_DISCREPANCY_AFR	=  0,
          FLOAT_DISCREPANCY_AFR	=  0,
          FLOAT_HOLDING_AFR	=  0,
          INDEPENDENT_COUNTS_AFR	=  0,
          HAND_OVER_COUNTS_AFR	=  0,
          SELF_COUNTS_AFR	=  0,
          SPEC_DEPT_REVENUE_BUDGET_AFR	=  0,
          SPEC_DPT_REV_BUDGET_IVAT_AFR	=  0,
          SPEC_DEPT_REVENUE_QTY_BDG_AFR	=  0,
          SPEC_DPT_REV_QTY_BDG_IVAT_AFR	=  0 
       
  WHERE  FIN_YEAR_NO >= (SELECT TODAY_FIN_YEAR_NO FROM DIM_CONTROL) 
  AND    (SELECT COUNT(*) FROM fnd_rtl_loc_dept_wk_fin_budg WHERE LAST_UPDATED_DATE = G_DATE) > 1000   ;
 
  l_text :='CLEAR CURRENT YEAR FORWARD COMPLETED  '||sql%rowcount||'  '||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  COMMIT;
        
    

--**************************************************************************************************
    open c_fnd_rtl_loc_dept_wk_fin_budg;
    fetch c_fnd_rtl_loc_dept_wk_fin_budg bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_loc_dept_wk_fin_budg bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_dept_wk_fin_budg;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
end wh_prf_corp_552u;
