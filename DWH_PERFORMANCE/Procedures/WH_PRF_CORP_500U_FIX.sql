--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_500U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_500U_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      Alastair de Wet
--  Purpose:     Create LIWk Dense rollup fact table in the performance layer
--               with input ex lid dense table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_dense
--               Output - rtl_loc_item_wk_rms_dense
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  20 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
--  06 Aug 2009 - Replaced Merge with Insert into select from with a generic partition truncate prior to run.
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_500U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS DENSE PERFORMANCE to WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_dense EX DAY LEVEL STARTED AT '||
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

    execute immediate 'alter session enable parallel dml';

    SELECT MIN(POST_DATE), MAX(POST_DATE) 
      INTO g_start_date, g_end_date
      FROM W6005682.dense_flowers_insert
      ;


MERGE /*+ PARALLEL(rtl_liwrd,4) */ INTO DWH_PERFORMANCE.RTL_LOC_ITEM_WK_RMS_DENSE rtl_liwrd USING
(
   select lid.sk1_location_no sk1_location_no,
          lid.sk1_item_no sk1_item_no,
          dc.fin_year_no fin_year_no,
          dc.fin_week_no fin_week_no,
          max(dc.fin_week_code) fin_week_code,
          max(dc.this_week_start_date) this_week_start_date,
          max(lid.sk2_location_no) sk2_location_no,
          max(lid.sk2_item_no) sk2_item_no ,
          sum(nvl(lid.sales_qty,0)) sales_qty,
          sum(nvl(lid.sales_cases,0)) sales_cases,
          sum(nvl(lid.sales,0)) sales,
          sum(nvl(lid.sales_incl_vat,0)) sales_incl_vat,
          sum(nvl(lid.sales_cost,0)) sales_cost,
          sum(nvl(lid.sales_fr_cost,0)) sales_fr_cost,
          sum(nvl(lid.sales_margin,0)) sales_margin,
          sum(nvl(lid.franchise_sales,0)) franchise_sales,
          sum(nvl(lid.franchise_sales_margin,0)) franchise_sales_margin,
          sum(nvl(lid.reg_sales_qty,0)) reg_sales_qty,
          sum(nvl(lid.reg_sales,0)) reg_sales,
          sum(nvl(lid.reg_sales_cost,0)) reg_sales_cost,
          sum(nvl(lid.reg_sales_fr_cost,0)) reg_sales_fr_cost,
          sum(nvl(lid.reg_sales_margin,0)) reg_sales_margin,
          sum(nvl(lid.franchise_reg_sales_margin,0)) franchise_reg_sales_margin,
          sum(nvl(lid.gross_sales_qty,0)) gross_sales_qty,
          sum(nvl(lid.gross_sales,0)) gross_sales,
          sum(nvl(lid.gross_sales_cost,0)) gross_sales_cost,
          sum(nvl(lid.gross_sales_fr_cost,0)) gross_sales_fr_cost,
          sum(nvl(lid.gross_reg_sales_qty,0)) gross_reg_sales_qty,
          sum(nvl(lid.gross_reg_sales,0)) gross_reg_sales,
          sum(nvl(lid.gross_reg_sales_cost,0)) gross_reg_sales_cost,
          sum(nvl(lid.gross_reg_sales_fr_cost,0)) gross_reg_sales_fr_cost,
          sum(nvl(lid.sdn_in_qty,0)) sdn_in_qty,
          sum(nvl(lid.sdn_in_selling,0)) sdn_in_selling,
          sum(nvl(lid.sdn_in_cost,0)) sdn_in_cost,
          sum(nvl(lid.sdn_in_fr_cost,0)) sdn_in_fr_cost,
          sum(nvl(lid.sdn_in_cases,0)) sdn_in_cases,
          sum(nvl(lid.actl_store_rcpt_qty,0)) actl_store_rcpt_qty,
          sum(nvl(lid.actl_store_rcpt_selling,0)) actl_store_rcpt_selling,
          sum(nvl(lid.actl_store_rcpt_cost,0)) actl_store_rcpt_cost,
          sum(nvl(lid.actl_store_rcpt_fr_cost,0)) actl_store_rcpt_fr_cost,
          sum(nvl(lid.store_deliv_selling,0)) store_deliv_selling,
          sum(nvl(lid.store_deliv_cost,0)) store_deliv_cost,
          sum(nvl(lid.store_deliv_fr_cost,0)) store_deliv_fr_cost,
          sum(nvl(lid.store_intake_qty,0)) store_intake_qty,
          sum(nvl(lid.store_intake_selling,0)) store_intake_selling,
          sum(nvl(lid.store_intake_cost,0)) store_intake_cost,
          sum(nvl(lid.store_intake_fr_cost,0)) store_intake_fr_cost,
          sum(nvl(lid.store_intake_margin,0)) store_intake_margin,
          sum(nvl(lid.sales_returns_qty,0)) sales_returns_qty,
          sum(nvl(lid.sales_returns_selling,0)) sales_returns_selling,
          sum(nvl(lid.sales_returns_cost,0)) sales_returns_cost,
          sum(nvl(lid.sales_returns_fr_cost,0)) sales_returns_fr_cost,
          sum(nvl(lid.reg_sales_returns_qty,0)) reg_sales_returns_qty,
          sum(nvl(lid.reg_sales_returns_selling,0)) reg_sales_returns_selling,
          sum(nvl(lid.reg_sales_returns_cost,0)) reg_sales_returns_cost,
          sum(nvl(lid.reg_sales_returns_fr_cost,0)) reg_sales_returns_fr_cost,
          sum(nvl(lid.clear_sales_returns_selling,0)) clear_sales_returns_selling,
          sum(nvl(lid.clear_sales_returns_cost,0)) clear_sales_returns_cost,
          sum(nvl(lid.clear_sales_returns_fr_cost,0)) clear_sales_returns_fr_cost,
          sum(nvl(lid.clear_sales_returns_qty,0)) clear_sales_returns_qty,
          max(lid.last_updated_date) last_updated_date,
          sum(nvl(lid.store_deliv_qty,0)) store_deliv_qty,
          sum(nvl(lid.store_deliv_cases,0)) store_deliv_cases,
          sum(nvl(lid.eol_sales,0)) eol_sales,
          sum(nvl(lid.eol_sales_qty,0)) eol_sales_qty,
          sum(nvl(lid.eol_discount,0)) eol_discount

   from   W6005682.dense_flowers_insert lid,
          dim_calendar dc
   where  lid.post_date         = dc.calendar_date and
          lid.post_date          between g_start_date and g_end_date
   group by dc.fin_year_no,dc.fin_week_no,
            lid.sk1_item_no,lid.sk1_location_no
            
  ) mer_dense
  
  on (rtl_liwrd.sk1_location_no = mer_dense.sk1_location_no
  and rtl_liwrd.sk1_item_no     = mer_dense.sk1_item_no
  and rtl_liwrd.fin_year_no     = mer_dense.fin_year_no
  and rtl_liwrd.fin_week_no     = mer_dense.fin_week_no
  )
  
  when matched then 
    update
      set 
      
          fin_week_code               =		              mer_dense.fin_week_code,
          this_week_start_date        =				          mer_dense.this_week_start_date,
          sk2_location_no             =				          mer_dense.sk2_location_no,
          sk2_item_no                 =				          mer_dense.sk2_item_no ,
          sales_qty                   =				          mer_dense.sales_qty,
          sales_cases                 =				          mer_dense.sales_cases,
          sales                       =				          mer_dense.sales,
          sales_incl_vat              =				          mer_dense.sales_incl_vat,
          sales_cost                  =				          mer_dense.sales_cost,
          sales_fr_cost               =				          mer_dense.sales_fr_cost,
          sales_margin                =				          mer_dense.sales_margin,
          franchise_sales             =				          mer_dense.franchise_sales,
          franchise_sales_margin      =				          mer_dense.franchise_sales_margin,
          reg_sales_qty               =				          mer_dense.reg_sales_qty,
          reg_sales                   =				          mer_dense.reg_sales,
          reg_sales_cost              =				          mer_dense.reg_sales_cost,
          reg_sales_fr_cost           =				          mer_dense.reg_sales_fr_cost,
          reg_sales_margin            =				          mer_dense.reg_sales_margin,
          franchise_reg_sales_margin  =				          mer_dense.franchise_reg_sales_margin,
          gross_sales_qty             =				          mer_dense.gross_sales_qty,
          gross_sales                 =				          mer_dense.gross_sales,
          gross_sales_cost            =				          mer_dense.gross_sales_cost,
          gross_sales_fr_cost         =				          mer_dense.gross_sales_fr_cost,
          gross_reg_sales_qty         =				          mer_dense.gross_reg_sales_qty,
          gross_reg_sales             =				          mer_dense.gross_reg_sales,
          gross_reg_sales_cost        =				          mer_dense.gross_reg_sales_cost,
          gross_reg_sales_fr_cost     =				          mer_dense.gross_reg_sales_fr_cost,
          sdn_in_qty                  =				          mer_dense.sdn_in_qty,
          sdn_in_selling              =				          mer_dense.sdn_in_selling,
          sdn_in_cost                 =				          mer_dense.sdn_in_cost,
          sdn_in_fr_cost              =				          mer_dense.sdn_in_fr_cost,
          sdn_in_cases                =				          mer_dense.sdn_in_cases,
          actl_store_rcpt_qty         =				          mer_dense.actl_store_rcpt_qty,
          actl_store_rcpt_selling     =				          mer_dense.actl_store_rcpt_selling,
          actl_store_rcpt_cost        =				          mer_dense.actl_store_rcpt_cost,
          actl_store_rcpt_fr_cost     =				          mer_dense.actl_store_rcpt_fr_cost,
          store_deliv_selling         =				          mer_dense.store_deliv_selling,
          store_deliv_cost            =				          mer_dense.store_deliv_cost,
          store_deliv_fr_cost         =				          mer_dense.store_deliv_fr_cost,
          store_intake_qty            =				          mer_dense.store_intake_qty,
          store_intake_selling        =				          mer_dense.store_intake_selling,
          store_intake_cost           =				          mer_dense.store_intake_cost,
          store_intake_fr_cost        =				          mer_dense.store_intake_fr_cost,
          store_intake_margin         =				          mer_dense.store_intake_margin,
          sales_returns_qty           =		              mer_dense.sales_returns_qty,
          sales_returns_selling       =				          mer_dense.sales_returns_selling,
          sales_returns_cost          =				          mer_dense.sales_returns_cost,
          sales_returns_fr_cost       =				          mer_dense.sales_returns_fr_cost,
          reg_sales_returns_qty       =				          mer_dense.reg_sales_returns_qty,
          reg_sales_returns_selling   =				          mer_dense.reg_sales_returns_selling,
          reg_sales_returns_cost      =				          mer_dense.reg_sales_returns_cost,
          reg_sales_returns_fr_cost   =				          mer_dense.reg_sales_returns_fr_cost,
          clear_sales_returns_selling =				          mer_dense.clear_sales_returns_selling,
          clear_sales_returns_cost    =				          mer_dense.clear_sales_returns_cost,
          clear_sales_returns_fr_cost =				          mer_dense.clear_sales_returns_fr_cost,
          clear_sales_returns_qty     =				          mer_dense.clear_sales_returns_qty,
          last_updated_date            =				          mer_dense.last_updated_date,
          store_deliv_qty             =				          mer_dense.store_deliv_qty,
          store_deliv_cases           = 			          mer_dense.store_deliv_cases,
          eol_sales                   =				          mer_dense.eol_sales,
          eol_sales_qty               =				          mer_dense.eol_sales_qty,
          eol_discount                =				          mer_dense.eol_discount

      
  when not matched then 
    insert 
    (
          sk1_location_no,
          sk1_item_no,
          fin_year_no,
          fin_week_no,
          fin_week_code,
          this_week_start_date,
          sk2_location_no,
          sk2_item_no,
          sales_qty,
          sales_cases,
          sales,
          sales_incl_vat,
          sales_cost,
          sales_fr_cost,
          sales_margin,
          franchise_sales,
          franchise_sales_margin,
          reg_sales_qty,
          reg_sales,
          reg_sales_cost,
          reg_sales_fr_cost,
          reg_sales_margin,
          franchise_reg_sales_margin,
          gross_sales_qty,
          gross_sales,
          gross_sales_cost ,
          gross_sales_fr_cost,
          gross_reg_sales_qty,
          gross_reg_sales ,
          gross_reg_sales_cost,
          gross_reg_sales_fr_cost,
          sdn_in_qty,
          sdn_in_selling,
          sdn_in_cost,
          sdn_in_fr_cost ,
          sdn_in_cases   ,
          actl_store_rcpt_qty     ,
          actl_store_rcpt_selling ,
          actl_store_rcpt_cost   ,
          actl_store_rcpt_fr_cost  ,
          store_deliv_selling   ,
          store_deliv_cost    ,
          store_deliv_fr_cost,
          store_intake_qty   ,
          store_intake_selling    ,
          store_intake_cost     ,
          store_intake_fr_cost    ,
          store_intake_margin    ,
          sales_returns_qty      ,
          sales_returns_selling   ,
          sales_returns_cost      ,
          sales_returns_fr_cost   ,
          reg_sales_returns_qty    ,
          reg_sales_returns_selling  ,
          reg_sales_returns_cost    ,
          reg_sales_returns_fr_cost  ,
          clear_sales_returns_selling ,
          clear_sales_returns_cost    ,
          clear_sales_returns_fr_cost,
          clear_sales_returns_qty    ,
          last_updated_date           ,
          store_deliv_qty           ,
          store_deliv_cases        ,
          eol_sales               ,
          eol_sales_qty            ,
          eol_discount
    )
    values
    (
          mer_dense.sk1_location_no,
          mer_dense.sk1_item_no,
          mer_dense.fin_year_no,
          mer_dense.fin_week_no,
          mer_dense.fin_week_code,
          mer_dense.this_week_start_date,
          mer_dense.sk2_location_no,
          mer_dense.sk2_item_no ,
          mer_dense.sales_qty,
          mer_dense.sales_cases,
          mer_dense.sales,
          mer_dense.sales_incl_vat,
          mer_dense.sales_cost,
          mer_dense.sales_fr_cost,
          mer_dense.sales_margin,
          mer_dense.franchise_sales,
          mer_dense.franchise_sales_margin,
          mer_dense.reg_sales_qty,
          mer_dense.reg_sales,
          mer_dense.reg_sales_cost,
          mer_dense.reg_sales_fr_cost,
          mer_dense.reg_sales_margin,
          mer_dense.franchise_reg_sales_margin,
          mer_dense.gross_sales_qty,
          mer_dense.gross_sales,
          mer_dense.gross_sales_cost,
          mer_dense.gross_sales_fr_cost,
          mer_dense.gross_reg_sales_qty,
          mer_dense.gross_reg_sales,
          mer_dense.gross_reg_sales_cost,
          mer_dense.gross_reg_sales_fr_cost,
          mer_dense.sdn_in_qty,
          mer_dense.sdn_in_selling,
          mer_dense.sdn_in_cost,
          mer_dense.sdn_in_fr_cost,
          mer_dense.sdn_in_cases,
          mer_dense.actl_store_rcpt_qty,
          mer_dense.actl_store_rcpt_selling,
          mer_dense.actl_store_rcpt_cost,
          mer_dense.actl_store_rcpt_fr_cost,
          mer_dense.store_deliv_selling,
          mer_dense.store_deliv_cost,
          mer_dense.store_deliv_fr_cost,
          mer_dense.store_intake_qty,
          mer_dense.store_intake_selling,
          mer_dense.store_intake_cost,
          mer_dense.store_intake_fr_cost,
          mer_dense.store_intake_margin,
          mer_dense.sales_returns_qty,
          mer_dense.sales_returns_selling,
          mer_dense.sales_returns_cost,
          mer_dense.sales_returns_fr_cost,
          mer_dense.reg_sales_returns_qty,
          mer_dense.reg_sales_returns_selling,
          mer_dense.reg_sales_returns_cost,
          mer_dense.reg_sales_returns_fr_cost,
          mer_dense.clear_sales_returns_selling,
          mer_dense.clear_sales_returns_cost,
          mer_dense.clear_sales_returns_fr_cost,
          mer_dense.clear_sales_returns_qty,
          mer_dense.last_updated_date,
          mer_dense.store_deliv_qty,
          mer_dense.store_deliv_cases,
          mer_dense.eol_sales,
          mer_dense.eol_sales_qty,
          mer_dense.eol_discount
    )
    ;

  g_recs_read := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;


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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_500u_FIX;
