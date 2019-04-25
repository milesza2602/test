--------------------------------------------------------
--  DDL for Procedure WH_FND_MP_010U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MP_010U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alfonso Joshua
--  Purpose:     Create Weekly Department Plan Type table in the foundation layer
--               with input ex staging table from MP.
--  Tables:      Input  - stg_mp_chain_dept_wk_plan_cpy
--               Output - fnd_chain_dept_wk_plan
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Jan 2008 - defect 452  - Change to stg_mp_chain_dept_wk_plan_cpy structure
--              - defect 453  - Change to fnd_chain_dept_wk_plan structure
--  28 Feb 2012 - defect 4627 - Total Planning Project - add 4 new store measures
--                            - Remove 2 measures (pln_store_intk_fr_rpl_selling, pln_store_intk_fr_fast_selling)
--  06 Nov 2012 - defect ???? - Commitment - add 4 new store measures
--  26 Jan 2015 - defect 5043 - Total Planning Project - add 3 new measures and remove 4 measures

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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_mp_chain_dept_wk_plan_hsp.sys_process_msg%type;
g_rec_out            fnd_chain_dept_wk_plan%rowtype;
g_rec_in             stg_mp_chain_dept_wk_plan_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MP_010U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WEEKLY DEPT PLAN TYPE EX MP';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_mp_chain_dept_wk_plan_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_chain_dept_wk_plan%rowtype index by binary_integer;
type tbl_array_u is table of fnd_chain_dept_wk_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_mp_chain_dept_wk_plan_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_mp_chain_dept_wk_plan_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_mp_chain_dept_wk_plan is
   select *
   from stg_mp_chain_dept_wk_plan_cpy;
--   where sys_process_code = 'N'
--   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
v_count              number              :=  0;

begin

   g_hospital                                := 'N';

   g_rec_out.chain_no                        := g_rec_in.chain_no;
   g_rec_out.department_no                   := g_rec_in.department_no;
   g_rec_out.plan_type_no                    := g_rec_in.plan_type_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.pln_sales_qty                   := g_rec_in.pln_sales_qty;
   g_rec_out.pln_sales                       := g_rec_in.pln_sales;
   g_rec_out.pln_sales_cost                  := g_rec_in.pln_sales_cost;
   g_rec_out.pln_net_mkdn                    := g_rec_in.pln_net_mkdn;
   g_rec_out.pln_store_opening_stk_qty       := g_rec_in.pln_store_opening_stk_qty;
   g_rec_out.pln_store_opening_stk_selling   := g_rec_in.pln_store_opening_stk_selling;
   g_rec_out.pln_store_opening_stk_cost      := g_rec_in.pln_store_opening_stk_cost;
   g_rec_out.pln_store_closing_stk_qty       := g_rec_in.pln_store_closing_stk_qty;
   g_rec_out.pln_store_closing_stk_selling   := g_rec_in.pln_store_closing_stk_selling;
   g_rec_out.pln_store_closing_stk_cost      := g_rec_in.pln_store_closing_stk_cost;
   g_rec_out.pln_store_intk_qty              := g_rec_in.pln_store_intk_qty;
   g_rec_out.pln_store_intk_selling          := g_rec_in.pln_store_intk_selling;
   g_rec_out.pln_store_intk_cost             := g_rec_in.pln_store_intk_cost;
-- QC-5004
   g_rec_out.pln_sales_margin                := g_rec_in.pln_sales_margin;
-- Remove 2 measures
--   g_rec_out.pln_store_intk_fr_rpl_selling   := g_rec_in.pln_store_intk_fr_rpl_selling;
--   g_rec_out.pln_store_intk_fr_fast_selling  := g_rec_in.pln_store_intk_fr_fast_selling;
-- remove 4 measures for Total Planning - March release 2015 - QC 5043
--   g_rec_out.pln_store_intk_fr_selling       := g_rec_in.pln_store_intk_fr_selling;
--   g_rec_out.pln_store_latest_order_fr_sell  := g_rec_in.pln_store_latest_order_fr_sell;
--   g_rec_out.pln_store_inter_fr_selling      := g_rec_in.pln_store_inter_fr_selling;
--   g_rec_out.pln_store_inter_jv_selling      := g_rec_in.pln_store_inter_jv_selling;
--
   g_rec_out.pln_store_inter_afr_selling     := g_rec_in.pln_store_inter_afr_selling;
   g_rec_out.pln_store_intk_tot_selling      := g_rec_in.pln_store_intk_tot_selling;
   g_rec_out.pln_chain_opening_stk_qty       := g_rec_in.pln_chain_opening_stk_qty;
   g_rec_out.pln_chain_opening_stk_selling   := g_rec_in.pln_chain_opening_stk_selling;
   g_rec_out.pln_chain_opening_stk_cost      := g_rec_in.pln_chain_opening_stk_cost;
   g_rec_out.pln_chain_closing_stk_qty       := g_rec_in.pln_chain_closing_stk_qty;
   g_rec_out.pln_chain_closing_stk_selling   := g_rec_in.pln_chain_closing_stk_selling;
   g_rec_out.pln_chain_closing_stk_cost      := g_rec_in.pln_chain_closing_stk_cost;
   g_rec_out.pln_chain_intk_selling          := g_rec_in.pln_chain_intk_selling;
   g_rec_out.pln_chn_local_commit_rpl_sell   := g_rec_in.pln_chn_local_commit_rpl_sell;
   g_rec_out.pln_chn_commit_wh_ord_selling   := g_rec_in.pln_chn_commit_wh_ord_selling;
   g_rec_out.pln_chain_rtv_qty               := g_rec_in.pln_chain_rtv_qty;
   g_rec_out.pln_chain_rtv_selling           := g_rec_in.pln_chain_rtv_selling;
   g_rec_out.pln_chain_otb_selling           := g_rec_in.pln_chain_otb_selling;
   g_rec_out.pln_chain_holdback_selling      := g_rec_in.pln_chain_holdback_selling;
   g_rec_out.pln_chain_release_plan_selling  := g_rec_in.pln_chain_release_plan_selling;
   g_rec_out.pln_chn_local_commit_fast_sell  := g_rec_in.pln_chn_local_commit_fast_sell;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.pln_chain_intk_qty               := g_rec_in.pln_chain_intk_qty;
   g_rec_out.pln_chain_intk_cost              := g_rec_in.pln_chain_intk_cost;
   g_rec_out.pln_chain_holdback_prc           := g_rec_in.pln_chain_holdback_prc;
   g_rec_out.pln_chain_total_commitment       := g_rec_in.pln_chain_total_commitment;
-- 4 new measures Commitment
   g_rec_out.pln_chn_commit_wh_ord_cost      := g_rec_in.pln_chn_commit_wh_ord_cost;
   g_rec_out.pln_chn_local_commit_fast_cost  := g_rec_in.pln_chn_local_commit_fast_cost;
   g_rec_out.pln_chn_local_commit_rpl_cost   := g_rec_in.pln_chn_local_commit_rpl_cost;
   g_rec_out.pln_chain_total_commit_cost     := g_rec_in.pln_chain_total_commit_cost;
-- 3 new measures Total Planning - March release 2015  QC 5043
   g_rec_out.pln_store_clr_mkdn_selling      := g_rec_in.pln_store_clr_mkdn_selling;
   g_rec_out.pln_store_prom_mkdn_selling     := g_rec_in.pln_store_prom_mkdn_selling;
   g_rec_out.pln_store_price_adjustment      := g_rec_in.pln_store_price_adjustment;

   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_source_code;
     l_text := dwh_constants.vc_invalid_source_code||' '||g_rec_out.source_data_status_code;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_chain(g_rec_out.chain_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_chain_not_found;
     l_text := dwh_constants.vc_chain_not_found||' '||g_rec_out.chain_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not  dwh_valid.fnd_department(g_rec_out.department_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_dept_not_found;
     l_text := dwh_constants.vc_dept_not_found||' '||g_rec_out.department_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if g_rec_out.plan_type_no not in (50,51,52,53,54,55) then
     g_hospital      := 'Y';
     g_hospital_text := 'INVALID PLAN TYPE VALUES ';
     l_text          := 'INVALID PLAN TYPE VALUES '||g_rec_out.plan_type_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.dim_dept_child_hierarchy(g_rec_out.department_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_dept_child_not_found;
     l_text := dwh_constants.vc_dept_child_not_found||' '||g_rec_out.department_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   select count(1)
   into v_count
   from fnd_calendar
   where fin_year_no = g_rec_in.fin_year_no
   and   fin_week_no = g_rec_in.fin_week_no;

   if v_count = 0 then
      g_hospital      := 'Y';
      g_hospital_text := 'INVALID FIN YEAR OR WEEK - FND_CALENDAR CONTAINS VALID VALUES ';
      l_text          := 'INVALID FIN YEAR OR WEEK - FND_CALENDAR CONTAINS VALID VALUES '||g_rec_out.fin_year_no||' '||g_rec_out.fin_week_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   select count(1)
   into v_count
   from fnd_plan_type
   where plan_type_no = g_rec_in.plan_type_no;

   if v_count = 0 then
      g_hospital      := 'Y';
      g_hospital_text := 'INVALID PLAN TYPE VALUES ';
      l_text          := 'INVALID PLAN TYPE VALUES '||g_rec_out.plan_type_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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

   insert into stg_mp_chain_dept_wk_plan_hsp values g_rec_in;
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
       insert into fnd_chain_dept_wk_plan values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).chain_no||
                       ' '||a_tbl_insert(g_error_index).department_no||
                       ' '||a_tbl_insert(g_error_index).plan_type_no||
                       ' '||a_tbl_insert(g_error_index).fin_year_no ||
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
       update fnd_chain_dept_wk_plan
       set    pln_sales_qty                  = a_tbl_update(i).pln_sales_qty,
              pln_sales                      = a_tbl_update(i).pln_sales,
              pln_sales_cost                 = a_tbl_update(i).pln_sales_cost,
              pln_net_mkdn                   = a_tbl_update(i).pln_net_mkdn,
              pln_store_opening_stk_qty      = a_tbl_update(i).pln_store_opening_stk_qty,
              pln_store_opening_stk_selling  = a_tbl_update(i).pln_store_opening_stk_selling,
              pln_store_opening_stk_cost     = a_tbl_update(i).pln_store_opening_stk_cost,
              pln_store_closing_stk_qty      = a_tbl_update(i).pln_store_closing_stk_qty,
              pln_store_closing_stk_selling  = a_tbl_update(i).pln_store_closing_stk_selling,
              pln_store_closing_stk_cost     = a_tbl_update(i).pln_store_closing_stk_cost,
              pln_store_intk_qty             = a_tbl_update(i).pln_store_intk_qty,
              pln_store_intk_selling         = a_tbl_update(i).pln_store_intk_selling,
              pln_store_intk_cost            = a_tbl_update(i).pln_store_intk_cost,
--              pln_store_intk_fr_rpl_selling  = a_tbl_update(i).pln_store_intk_fr_rpl_selling,
--              pln_store_intk_fr_fast_selling = a_tbl_update(i).pln_store_intk_fr_fast_selling,
-- remove 4 measures for Total Planning - March release 2015 QC 5043
--              pln_store_intk_fr_selling      = a_tbl_update(i).pln_store_intk_fr_selling,
--              pln_store_latest_order_fr_sell = a_tbl_update(i).pln_store_latest_order_fr_sell,
--              pln_store_inter_fr_selling     = a_tbl_update(i).pln_store_inter_fr_selling,
--              pln_store_inter_jv_selling     = a_tbl_update(i).pln_store_inter_jv_selling,
--
              pln_store_intk_tot_selling     = a_tbl_update(i).pln_store_intk_tot_selling,
              pln_store_inter_afr_selling    = a_tbl_update(i).pln_store_inter_afr_selling,
              pln_chain_opening_stk_qty      = a_tbl_update(i).pln_chain_opening_stk_qty,
              pln_chain_opening_stk_selling  = a_tbl_update(i).pln_chain_opening_stk_selling,
              pln_chain_opening_stk_cost     = a_tbl_update(i).pln_chain_opening_stk_cost,
              pln_chain_closing_stk_qty      = a_tbl_update(i).pln_chain_closing_stk_qty,
              pln_chain_closing_stk_selling  = a_tbl_update(i).pln_chain_closing_stk_selling,
              pln_chain_closing_stk_cost     = a_tbl_update(i).pln_chain_closing_stk_cost,
              pln_chain_intk_selling         = a_tbl_update(i).pln_chain_intk_selling,
              pln_chn_local_commit_rpl_sell  = a_tbl_update(i).pln_chn_local_commit_rpl_sell,
              pln_chn_commit_wh_ord_selling  = a_tbl_update(i).pln_chn_commit_wh_ord_selling,
              pln_chain_rtv_qty              = a_tbl_update(i).pln_chain_rtv_qty,
              pln_chain_rtv_selling          = a_tbl_update(i).pln_chain_rtv_selling,
              pln_chain_otb_selling          = a_tbl_update(i).pln_chain_otb_selling,
              pln_chain_holdback_selling     = a_tbl_update(i).pln_chain_holdback_selling,
              pln_chain_release_plan_selling = a_tbl_update(i).pln_chain_release_plan_selling,
              pln_chn_local_commit_fast_sell = a_tbl_update(i).pln_chn_local_commit_fast_sell,
              source_data_status_code        = a_tbl_update(i).source_data_status_code,
              last_updated_date              = a_tbl_update(i).last_updated_date,
              pln_chain_intk_qty             = a_tbl_update(i).pln_chain_intk_qty,
              pln_chain_intk_cost            = a_tbl_update(i).pln_chain_intk_cost,
              pln_chain_holdback_prc         = a_tbl_update(i).pln_chain_holdback_prc,
              pln_chain_total_commitment     = a_tbl_update(i).pln_chain_total_commitment,
              pln_chn_commit_wh_ord_cost     = a_tbl_update(i).pln_chn_commit_wh_ord_cost,
              pln_chn_local_commit_fast_cost = a_tbl_update(i).pln_chn_local_commit_fast_cost,
              pln_chn_local_commit_rpl_cost  = a_tbl_update(i).pln_chn_local_commit_rpl_cost,
              pln_chain_total_commit_cost    = a_tbl_update(i).pln_chain_total_commit_cost,
              pln_sales_margin               = a_tbl_update(i).pln_sales_margin,
-- 3 new measures Total Planning - March release 2015 QC 5043
              pln_store_clr_mkdn_selling     = a_tbl_update(i).pln_store_clr_mkdn_selling,
              pln_store_prom_mkdn_selling    = a_tbl_update(i).pln_store_prom_mkdn_selling,
              pln_store_price_adjustment     = a_tbl_update(i).pln_store_price_adjustment

       where  chain_no                       = a_tbl_update(i).chain_no and
              department_no                  = a_tbl_update(i).department_no and
              plan_type_no                   = a_tbl_update(i).plan_type_no and
              fin_year_no                    = a_tbl_update(i).fin_year_no and
              fin_week_no                    = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).chain_no ||
                       ' '||a_tbl_update(g_error_index).department_no ||
                       ' '||a_tbl_update(g_error_index).plan_type_no  ||
                       ' '||a_tbl_update(g_error_index).fin_year_no ||
                       ' '||a_tbl_update(g_error_index).fin_week_no ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_mp_chain_dept_wk_plan_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_chain_dept_wk_plan
   where  chain_no        = g_rec_out.chain_no      and
          department_no   = g_rec_out.department_no and
          plan_type_no    = g_rec_out.plan_type_no  and
          fin_year_no     = g_rec_out.fin_year_no   and
          fin_week_no     = g_rec_out.fin_week_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).chain_no      = g_rec_out.chain_no       and
            a_tbl_insert(i).department_no = g_rec_out.department_no  and
            a_tbl_insert(i).plan_type_no  = g_rec_out.plan_type_no   and
            a_tbl_insert(i).fin_year_no   = g_rec_out.fin_year_no    and
            a_tbl_insert(i).fin_week_no   = g_rec_out.fin_week_no   then
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
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;
      local_bulk_staging_update;

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
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum  then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_CHAIN_DEPT_WK_PLAN EX MP STARTED AT '||
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
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_mp_chain_dept_wk_plan;
    fetch c_stg_mp_chain_dept_wk_plan bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_mp_chain_dept_wk_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_mp_chain_dept_wk_plan;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;


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
end wh_fnd_mp_010u;
