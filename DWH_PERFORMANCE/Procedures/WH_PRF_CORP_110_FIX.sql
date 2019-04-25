--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_110_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_110_FIX" 
                                    (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID dense sales fact table in the performance layer
--               with input ex RMS Sale table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_rms_sale
--               Output - rtl_loc_item_dy_rms_dense
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  13 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
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
g_recs_read          number       :=  0;
g_recs_inserted      number       :=  0;
g_recs_updated       number       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_rec_out            rtl_loc_item_dy_rms_dense%rowtype;

g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_wac                rtl_loc_item_dy_rms_price.wac%type             := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_110U_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS DENSE SALES FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_m is table of rtl_loc_item_dy_rms_dense%rowtype index by binary_integer;

a_tbl_merge        tbl_array_m;
a_empty_set_m       tbl_array_m;

a_count             number       := 0;
a_count_m          number       := 0;
g_process_no number   := 0;
g_from_loc_no number  := 0;
g_to_loc_no   number  := 0;

cursor c_fnd_rtl_loc_item_dy_rms_sale is
   select /*+ full(di) full(dl) full(dlh) full(dih) full(fnd_li) parallel (fnd_li,6)  */  
          post_date,
          sales_qty,
          sales,
          sales_cost,
          reg_sales_qty,
          reg_sales,
          reg_sales_cost,
          sdn_in_qty,
          sdn_in_selling,
          sdn_in_cost,
          sales_returns_qty,
          sales_returns,
          sales_returns_cost,
          grn_qty,
          grn_selling,
          grn_cost,
          invoice_adj_qty,
          invoice_adj_selling,
          invoice_adj_cost,
          prom_discount_no,
          di.standard_uom_code,di.business_unit_no,di.vat_rate_perc,di.sk1_department_no,di.sk1_item_no,
          dl.chain_no,dl.sk1_location_no,dl.loc_type,
          decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray) num_units_per_tray,
          nvl(fnd_li.clearance_ind,0) clearance_ind,
          dih.sk2_item_no,
          dlh.sk2_location_no
   from   fnd_rtl_loc_item_dy_rms_sale fnd_lid,
          dim_item di,
          dim_location dl,
          fnd_location_item fnd_li,
          dim_item_hist dih,
          dim_location_hist dlh
   where  --fnd_lid.last_updated_date  = g_date and
          fnd_lid.post_date  = '25 OCT 2014' AND
--          fnd_lid.location_no        between p_from_loc_no and p_to_loc_no and
          fnd_lid.location_no        between g_from_loc_no and g_to_loc_no and
          fnd_lid.item_no            = di.item_no and
          fnd_lid.location_no        = dl.location_no and
          fnd_lid.item_no            = dih.item_no and
          fnd_lid.post_date          between dih.sk2_active_from_date and dih.sk2_active_to_date and
          fnd_lid.location_no        = dlh.location_no and
          fnd_lid.post_date          between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          fnd_lid.item_no            = fnd_li.item_no(+) and
          fnd_lid.location_no        = fnd_li.location_no(+) and
          ((
          fnd_lid.sales_qty         ||
          fnd_lid.reg_sales_qty     ||
          fnd_lid.sdn_in_qty        ||
          fnd_lid.sales_returns_qty ||
          fnd_lid.grn_qty           ||
          fnd_lid.invoice_adj_qty   ||
          fnd_lid.prom_discount_no) is not null
          );


g_rec_in                   c_fnd_rtl_loc_item_dy_rms_sale%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_dy_rms_sale%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                                 := null;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sales_qty                       := g_rec_in.sales_qty;
   g_rec_out.sales                           := g_rec_in.sales;
   g_rec_out.sales_cost                      := g_rec_in.sales_cost;
   g_rec_out.reg_sales_qty                   := g_rec_in.reg_sales_qty;
   g_rec_out.reg_sales                       := g_rec_in.reg_sales;
   g_rec_out.reg_sales_cost                  := g_rec_in.reg_sales_cost;
   g_rec_out.sdn_in_qty                      := g_rec_in.sdn_in_qty;
   g_rec_out.sdn_in_selling                  := g_rec_in.sdn_in_selling;
   g_rec_out.sdn_in_cost                     := g_rec_in.sdn_in_cost;
   g_rec_out.sales_returns_qty               := g_rec_in.sales_returns_qty;
   g_rec_out.sales_returns_selling           := g_rec_in.sales_returns;
   g_rec_out.sales_returns_cost              := g_rec_in.sales_returns_cost;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;

-- Value add and calculated fields added to performance layer
   g_rec_out.sales_cases                  := '';
   g_rec_out.sdn_in_cases                 := '';
   g_rec_out.sales_fr_cost                := '';
   g_rec_out.franchise_sales              := '';
   g_rec_out.franchise_sales_margin       := '';
   g_rec_out.reg_sales_fr_cost            := '';
   g_rec_out.franchise_reg_sales_margin   := '';
   g_rec_out.sdn_in_fr_cost               := '';
   g_rec_out.store_deliv_fr_cost          := '';
   g_rec_out.store_deliv_cases            := '';
   g_rec_out.store_deliv_selling          := '';
   g_rec_out.store_deliv_cost             := '';
   g_rec_out.store_deliv_qty              := '';
   g_rec_out.store_intake_fr_cost         := '';
   g_rec_out.sales_returns_fr_cost        := '';
   g_rec_out.reg_sales_returns_fr_cost    := '';
   g_rec_out.clear_sales_returns_fr_cost  := '';
   g_rec_out.gross_sales_fr_cost          := '';
   g_rec_out.gross_reg_sales_fr_cost      := '';
   g_rec_out.reg_sales_returns_qty        := '';
   g_rec_out.reg_sales_returns_selling    := '';
   g_rec_out.reg_sales_returns_cost       := '';
   g_rec_out.clear_sales_returns_qty      := '';
   g_rec_out.clear_sales_returns_selling  := '';
   g_rec_out.clear_sales_returns_cost     := '';
g_process_no := 1;
   g_rec_out.sales_incl_vat        := round(nvl(g_rec_out.sales,0) * (100 + g_rec_in.vat_rate_perc) / 100,2);
   g_rec_out.sales_margin          := nvl(g_rec_out.sales,0) - nvl(g_rec_out.sales_cost,0);
   g_rec_out.reg_sales_margin      := nvl(g_rec_out.reg_sales,0) - nvl(g_rec_out.reg_sales_cost,0);
   g_rec_out.gross_sales_qty       := nvl(g_rec_out.sales_qty,0) + nvl(g_rec_out.sales_returns_qty,0);
   g_rec_out.gross_sales           := nvl(g_rec_out.sales,0) + nvl(g_rec_out.sales_returns_selling,0);
   if g_rec_in.loc_type = 'S' then
      g_rec_out.store_deliv_selling   := nvl(g_rec_out.sdn_in_selling,0) + nvl(g_rec_in.grn_selling,0);
      g_rec_out.store_deliv_cost      := nvl(g_rec_out.sdn_in_cost,0) + nvl(g_rec_in.grn_cost,0);
      g_rec_out.store_deliv_qty       := nvl(g_rec_out.sdn_in_qty,0)  + nvl(g_rec_in.grn_qty,0)  ;
   end if;
g_process_no := 2;
   if g_rec_in.loc_type = 'S' then
      g_rec_out.store_intake_qty      := nvl(g_rec_out.sdn_in_qty,0) + nvl(g_rec_in.grn_qty,0) + nvl(g_rec_in.invoice_adj_qty,0);
      g_rec_out.store_intake_selling  := nvl(g_rec_out.sdn_in_selling,0) + nvl(g_rec_in.grn_selling,0) + nvl(g_rec_in.invoice_adj_selling,0);
      g_rec_out.store_intake_cost     := nvl(g_rec_out.sdn_in_cost,0) + nvl(g_rec_in.grn_cost,0) + nvl(g_rec_in.invoice_adj_cost,0);
      g_rec_out.store_intake_margin   := nvl(g_rec_out.store_intake_selling,0) - nvl(g_rec_out.store_intake_cost,0);
   end if;
g_process_no := 3;
   if g_rec_in.clearance_ind = 0 and g_rec_in.prom_discount_no is null then
      g_rec_out.reg_sales_returns_qty     :=  g_rec_out.sales_returns_qty ;
      g_rec_out.reg_sales_returns_selling :=  g_rec_out.sales_returns_selling  ;
      g_rec_out.reg_sales_returns_cost    :=  g_rec_out.sales_returns_cost ;
   end if;
g_process_no := 4;
   if g_rec_in.clearance_ind = 1  then
      g_rec_out.clear_sales_returns_qty     :=  g_rec_out.sales_returns_qty ;
      g_rec_out.clear_sales_returns_selling :=  g_rec_out.sales_returns_selling  ;
      g_rec_out.clear_sales_returns_cost    :=  g_rec_out.sales_returns_cost ;
   end if;
g_process_no := 5;
   g_rec_out.gross_sales_cost           := nvl(g_rec_out.sales_cost,0) + nvl(g_rec_out.sales_returns_cost,0);
   g_rec_out.gross_reg_sales_qty        := nvl(g_rec_out.reg_sales_qty,0) + nvl(g_rec_out.reg_sales_returns_qty,0);
   g_rec_out.gross_reg_sales            := nvl(g_rec_out.reg_sales,0) + nvl(g_rec_out.reg_sales_returns_selling,0);
   g_rec_out.gross_reg_sales_cost       := nvl(g_rec_out.reg_sales_cost,0) + nvl(g_rec_out.reg_sales_returns_cost,0);
g_process_no := 6;
-- Case quantities can not contain fractions, the case quantity has to be an integer value (ie. 976.0).
   if g_rec_in.business_unit_no = 50 then
      g_rec_out.sales_cases  := round((nvl(g_rec_out.sales_qty,0)/g_rec_in.num_units_per_tray),0);
      g_rec_out.sdn_in_cases := round((nvl(g_rec_out.sdn_in_qty,0)/g_rec_in.num_units_per_tray),0);
      if g_rec_in.loc_type = 'S' then
         g_rec_out.store_deliv_cases :=
         round((nvl(g_rec_in.grn_qty,0)/g_rec_in.num_units_per_tray),0) + g_rec_out.sdn_in_cases;
      end if;
   end if;
g_process_no := 7;
   if g_rec_in.chain_no = 20 then
      begin
         select debtors_commission_perc
         into   g_debtors_commission_perc
         from   rtl_loc_dept_dy
         where  sk1_location_no       = g_rec_out.sk1_location_no and
                sk1_department_no     = g_rec_in.sk1_department_no and
                post_date             = g_rec_out.post_date;
         exception
            when no_data_found then
              g_debtors_commission_perc := 0;
      end;
      if g_debtors_commission_perc is null then
         g_debtors_commission_perc := 0;
      end if;
  g_process_no := 8;
      g_rec_out.sales_fr_cost                := nvl(g_rec_out.sales_cost,0) + round((nvl(g_rec_out.sales_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_sales              := g_rec_out.sales;
      g_rec_out.franchise_sales_margin       := nvl(g_rec_out.franchise_sales,0) - nvl(g_rec_out.sales_fr_cost,0);
      g_rec_out.reg_sales_fr_cost            := nvl(g_rec_out.reg_sales_cost,0) + round((nvl(g_rec_out.reg_sales_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.franchise_reg_sales_margin   := nvl(g_rec_out.reg_sales,0) - nvl(g_rec_out.reg_sales_fr_cost,0);
      g_rec_out.sdn_in_fr_cost               := nvl(g_rec_out.sdn_in_cost,0) + round((nvl(g_rec_out.sdn_in_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.store_deliv_fr_cost          := nvl(g_rec_out.store_deliv_cost,0) + round((nvl(g_rec_out.store_deliv_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.sales_returns_fr_cost        := nvl(g_rec_out.sales_returns_cost,0) + round((nvl(g_rec_out.sales_returns_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.reg_sales_returns_fr_cost    := nvl(g_rec_out.reg_sales_returns_cost,0) + round((nvl(g_rec_out.reg_sales_returns_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.clear_sales_returns_fr_cost  := nvl(g_rec_out.clear_sales_returns_cost,0) + round((nvl(g_rec_out.clear_sales_returns_cost,0) * g_debtors_commission_perc / 100),2);
      g_rec_out.gross_sales_fr_cost          := nvl(g_rec_out.sales_fr_cost,0) + nvl(g_rec_out.sales_returns_fr_cost,0);
      g_rec_out.gross_reg_sales_fr_cost      := nvl(g_rec_out.reg_sales_fr_cost,0) + nvl(g_rec_out.reg_sales_returns_fr_cost,0);
g_process_no := 9;
      if g_rec_in.loc_type = 'S' then
         g_rec_out.store_intake_fr_cost      := nvl(g_rec_out.store_intake_cost,0) + round((nvl(g_rec_out.store_intake_cost,0)
                                                    * g_debtors_commission_perc / 100),2);
      end if;

   end if;

   exception
      when others then
l_text := '- abort--1--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_text := 'ERROR - g_process_no - '||g_process_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
l_text := '- abort--2--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--3--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin
    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions
MERGE /*+ USE_HASH(rtl_lidrd ,mer_rlidrd)*/ INTO RTL_LOC_ITEM_DY_RMS_DENSE rtl_lidrd
USING
(
SELECT a_tbl_merge(i).SK1_LOCATION_NO               AS  SK1_LOCATION_NO,
       a_tbl_merge(i).SK1_ITEM_NO                   AS  SK1_ITEM_NO,
       a_tbl_merge(i).POST_DATE                     AS  POST_DATE,
       a_tbl_merge(i).SK2_LOCATION_NO               AS  SK2_LOCATION_NO,
       a_tbl_merge(i).SK2_ITEM_NO                   AS  SK2_ITEM_NO,
       a_tbl_merge(i).SALES_QTY                     AS  SALES_QTY,
       a_tbl_merge(i).SALES_CASES                   AS  SALES_CASES,
       a_tbl_merge(i).SALES                         AS  SALES,
       a_tbl_merge(i).SALES_INCL_VAT                AS  SALES_INCL_VAT,
       a_tbl_merge(i).SALES_COST                    AS  SALES_COST,
       a_tbl_merge(i).SALES_FR_COST                 AS  SALES_FR_COST,
       a_tbl_merge(i).SALES_MARGIN                  AS  SALES_MARGIN,
       a_tbl_merge(i).FRANCHISE_SALES               AS  FRANCHISE_SALES,
       a_tbl_merge(i).FRANCHISE_SALES_MARGIN        AS  FRANCHISE_SALES_MARGIN,
       a_tbl_merge(i).REG_SALES_QTY                 AS  REG_SALES_QTY,
       a_tbl_merge(i).REG_SALES                     AS  REG_SALES,
       a_tbl_merge(i).REG_SALES_COST                AS  REG_SALES_COST,
       a_tbl_merge(i).REG_SALES_FR_COST             AS  REG_SALES_FR_COST,
       a_tbl_merge(i).REG_SALES_MARGIN              AS  REG_SALES_MARGIN,
       a_tbl_merge(i).FRANCHISE_REG_SALES_MARGIN    AS  FRANCHISE_REG_SALES_MARGIN,
       a_tbl_merge(i).GROSS_SALES_QTY               AS  GROSS_SALES_QTY,
       a_tbl_merge(i).GROSS_SALES                   AS  GROSS_SALES,
       a_tbl_merge(i).GROSS_SALES_COST              AS  GROSS_SALES_COST,
       a_tbl_merge(i).GROSS_SALES_FR_COST           AS  GROSS_SALES_FR_COST,
       a_tbl_merge(i).GROSS_REG_SALES_QTY           AS  GROSS_REG_SALES_QTY,
       a_tbl_merge(i).GROSS_REG_SALES               AS  GROSS_REG_SALES,
       a_tbl_merge(i).GROSS_REG_SALES_COST          AS  GROSS_REG_SALES_COST,
       a_tbl_merge(i).GROSS_REG_SALES_FR_COST       AS  GROSS_REG_SALES_FR_COST,
       a_tbl_merge(i).SDN_IN_QTY                    AS  SDN_IN_QTY,
       a_tbl_merge(i).SDN_IN_SELLING                AS  SDN_IN_SELLING,
       a_tbl_merge(i).SDN_IN_COST                   AS  SDN_IN_COST,
       a_tbl_merge(i).SDN_IN_FR_COST                AS  SDN_IN_FR_COST,
       a_tbl_merge(i).SDN_IN_CASES                  AS  SDN_IN_CASES,
       a_tbl_merge(i).ACTL_STORE_RCPT_QTY           AS  ACTL_STORE_RCPT_QTY,
       a_tbl_merge(i).ACTL_STORE_RCPT_SELLING       AS  ACTL_STORE_RCPT_SELLING,
       a_tbl_merge(i).ACTL_STORE_RCPT_COST          AS  ACTL_STORE_RCPT_COST,
       a_tbl_merge(i).ACTL_STORE_RCPT_FR_COST       AS  ACTL_STORE_RCPT_FR_COST,
       a_tbl_merge(i).STORE_DELIV_SELLING           AS  STORE_DELIV_SELLING,
       a_tbl_merge(i).STORE_DELIV_COST              AS  STORE_DELIV_COST,
       a_tbl_merge(i).STORE_DELIV_FR_COST           AS  STORE_DELIV_FR_COST,
       a_tbl_merge(i).STORE_DELIV_QTY               AS  STORE_DELIV_QTY,
       a_tbl_merge(i).STORE_DELIV_CASES             AS  STORE_DELIV_CASES,
       a_tbl_merge(i).STORE_INTAKE_QTY              AS  STORE_INTAKE_QTY,
       a_tbl_merge(i).STORE_INTAKE_SELLING          AS  STORE_INTAKE_SELLING,
       a_tbl_merge(i).STORE_INTAKE_COST             AS  STORE_INTAKE_COST,
       a_tbl_merge(i).STORE_INTAKE_FR_COST          AS  STORE_INTAKE_FR_COST,
       a_tbl_merge(i).STORE_INTAKE_MARGIN           AS  STORE_INTAKE_MARGIN,
       a_tbl_merge(i).SALES_RETURNS_QTY             AS  SALES_RETURNS_QTY,
       a_tbl_merge(i).SALES_RETURNS_SELLING         AS  SALES_RETURNS_SELLING,
       a_tbl_merge(i).SALES_RETURNS_COST            AS  SALES_RETURNS_COST,
       a_tbl_merge(i).SALES_RETURNS_FR_COST         AS  SALES_RETURNS_FR_COST,
       a_tbl_merge(i).REG_SALES_RETURNS_QTY         AS  REG_SALES_RETURNS_QTY,
       a_tbl_merge(i).REG_SALES_RETURNS_SELLING     AS  REG_SALES_RETURNS_SELLING,
       a_tbl_merge(i).REG_SALES_RETURNS_COST        AS  REG_SALES_RETURNS_COST,
       a_tbl_merge(i).REG_SALES_RETURNS_FR_COST     AS  REG_SALES_RETURNS_FR_COST,
       a_tbl_merge(i).CLEAR_SALES_RETURNS_SELLING   AS  CLEAR_SALES_RETURNS_SELLING,
       a_tbl_merge(i).CLEAR_SALES_RETURNS_COST      AS  CLEAR_SALES_RETURNS_COST,
       a_tbl_merge(i).CLEAR_SALES_RETURNS_FR_COST   AS  CLEAR_SALES_RETURNS_FR_COST,
       a_tbl_merge(i).CLEAR_SALES_RETURNS_QTY       AS  CLEAR_SALES_RETURNS_QTY,
       a_tbl_merge(i).LAST_UPDATED_DATE             AS  LAST_UPDATED_DATE
       FROM dual
) mer_rlidrd
ON
(mer_rlidrd.SK1_LOCATION_NO = rtl_lidrd.SK1_LOCATION_NO
and mer_rlidrd.SK1_ITEM_NO = rtl_lidrd.SK1_ITEM_NO
and mer_rlidrd.POST_DATE = rtl_lidrd.POST_DATE)
WHEN MATCHED
THEN
UPDATE
SET           sales_qty                       = mer_rlidrd.sales_qty,
              sales_cases                     = mer_rlidrd.sales_cases,
              sales                           = mer_rlidrd.sales,
              sales_incl_vat                  = mer_rlidrd.sales_incl_vat,
              sales_cost                      = mer_rlidrd.sales_cost,
              sales_fr_cost                   = mer_rlidrd.sales_fr_cost,
              sales_margin                    = mer_rlidrd.sales_margin,
              franchise_sales                 = mer_rlidrd.franchise_sales,
              franchise_sales_margin          = mer_rlidrd.franchise_sales_margin,
              reg_sales_qty                   = mer_rlidrd.reg_sales_qty,
              reg_sales                       = mer_rlidrd.reg_sales,
              reg_sales_cost                  = mer_rlidrd.reg_sales_cost,
              reg_sales_fr_cost               = mer_rlidrd.reg_sales_fr_cost,
              reg_sales_margin                = mer_rlidrd.reg_sales_margin,
              franchise_reg_sales_margin      = mer_rlidrd.franchise_reg_sales_margin,
              gross_sales_qty                 = mer_rlidrd.gross_sales_qty,
              gross_sales                     = mer_rlidrd.gross_sales,
              gross_sales_cost                = mer_rlidrd.gross_sales_cost,
              gross_sales_fr_cost             = mer_rlidrd.gross_sales_fr_cost,
              gross_reg_sales_qty             = mer_rlidrd.gross_reg_sales_qty,
              gross_reg_sales                 = mer_rlidrd.gross_reg_sales,
              gross_reg_sales_cost            = mer_rlidrd.gross_reg_sales_cost,
              gross_reg_sales_fr_cost         = mer_rlidrd.gross_reg_sales_fr_cost,
              sdn_in_qty                      = mer_rlidrd.sdn_in_qty,
              sdn_in_selling                  = mer_rlidrd.sdn_in_selling,
              sdn_in_cost                     = mer_rlidrd.sdn_in_cost,
              sdn_in_fr_cost                  = mer_rlidrd.sdn_in_fr_cost,
              sdn_in_cases                    = mer_rlidrd.sdn_in_cases,
              store_deliv_selling             = mer_rlidrd.store_deliv_selling,
              store_deliv_cost                = mer_rlidrd.store_deliv_cost,
              store_deliv_fr_cost             = mer_rlidrd.store_deliv_fr_cost,
              store_deliv_qty                 = mer_rlidrd.store_deliv_qty,
              store_deliv_cases               = mer_rlidrd.store_deliv_cases,
              store_intake_qty                = mer_rlidrd.store_intake_qty,
              store_intake_selling            = mer_rlidrd.store_intake_selling,
              store_intake_cost               = mer_rlidrd.store_intake_cost,
              store_intake_fr_cost            = mer_rlidrd.store_intake_fr_cost,
              store_intake_margin             = mer_rlidrd.store_intake_margin,
              sales_returns_qty               = mer_rlidrd.sales_returns_qty,
              sales_returns_selling           = mer_rlidrd.sales_returns_selling,
              sales_returns_cost              = mer_rlidrd.sales_returns_cost,
              sales_returns_fr_cost           = mer_rlidrd.sales_returns_fr_cost,
              reg_sales_returns_qty           = mer_rlidrd.reg_sales_returns_qty,
              reg_sales_returns_selling       = mer_rlidrd.reg_sales_returns_selling,
              reg_sales_returns_cost          = mer_rlidrd.reg_sales_returns_cost,
              reg_sales_returns_fr_cost       = mer_rlidrd.reg_sales_returns_fr_cost,
              clear_sales_returns_selling     = mer_rlidrd.clear_sales_returns_selling,
              clear_sales_returns_cost        = mer_rlidrd.clear_sales_returns_cost,
              clear_sales_returns_fr_cost     = mer_rlidrd.clear_sales_returns_fr_cost,
              clear_sales_returns_qty         = mer_rlidrd.clear_sales_returns_qty,
              last_updated_date               = mer_rlidrd.last_updated_date

WHEN NOT MATCHED
THEN
INSERT
(
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              POST_DATE,
              SK2_LOCATION_NO,
              SK2_ITEM_NO,
              SALES_QTY,
              SALES_CASES,
              SALES,
              SALES_INCL_VAT,
              SALES_COST,
              SALES_FR_COST,
              SALES_MARGIN,
              FRANCHISE_SALES,
              FRANCHISE_SALES_MARGIN,
              REG_SALES_QTY,
              REG_SALES,
              REG_SALES_COST,
              REG_SALES_FR_COST,
              REG_SALES_MARGIN,
              FRANCHISE_REG_SALES_MARGIN,
              GROSS_SALES_QTY,
              GROSS_SALES,
              GROSS_SALES_COST,
              GROSS_SALES_FR_COST,
              GROSS_REG_SALES_QTY,
              GROSS_REG_SALES,
              GROSS_REG_SALES_COST,
              GROSS_REG_SALES_FR_COST,
              SDN_IN_QTY,
              SDN_IN_SELLING,
              SDN_IN_COST,
              SDN_IN_FR_COST,
              SDN_IN_CASES,
              ACTL_STORE_RCPT_QTY,
              ACTL_STORE_RCPT_SELLING,
              ACTL_STORE_RCPT_COST,
              ACTL_STORE_RCPT_FR_COST,
              STORE_DELIV_SELLING,
              STORE_DELIV_COST,
              STORE_DELIV_FR_COST,
              STORE_INTAKE_QTY,
              STORE_INTAKE_SELLING,
              STORE_INTAKE_COST,
              STORE_INTAKE_FR_COST,
              STORE_INTAKE_MARGIN,
              SALES_RETURNS_QTY,
              SALES_RETURNS_SELLING,
              SALES_RETURNS_COST,
              SALES_RETURNS_FR_COST,
              REG_SALES_RETURNS_QTY,
              REG_SALES_RETURNS_SELLING,
              REG_SALES_RETURNS_COST,
              REG_SALES_RETURNS_FR_COST,
              CLEAR_SALES_RETURNS_SELLING,
              CLEAR_SALES_RETURNS_COST,
              CLEAR_SALES_RETURNS_FR_COST,
              CLEAR_SALES_RETURNS_QTY,
              LAST_UPDATED_DATE,
              STORE_DELIV_QTY,
              STORE_DELIV_CASES
)
VALUES
(
        mer_rlidrd.SK1_LOCATION_NO,
        mer_rlidrd.SK1_ITEM_NO,
        mer_rlidrd.POST_DATE,
        mer_rlidrd.SK2_LOCATION_NO,
        mer_rlidrd.SK2_ITEM_NO,
        mer_rlidrd.SALES_QTY,
        mer_rlidrd.SALES_CASES,
        mer_rlidrd.SALES,
        mer_rlidrd.SALES_INCL_VAT,
        mer_rlidrd.SALES_COST,
        mer_rlidrd.SALES_FR_COST,
        mer_rlidrd.SALES_MARGIN,
        mer_rlidrd.FRANCHISE_SALES,
        mer_rlidrd.FRANCHISE_SALES_MARGIN,
        mer_rlidrd.REG_SALES_QTY,
        mer_rlidrd.REG_SALES,
        mer_rlidrd.REG_SALES_COST,
        mer_rlidrd.REG_SALES_FR_COST,
        mer_rlidrd.REG_SALES_MARGIN,
        mer_rlidrd.FRANCHISE_REG_SALES_MARGIN,
        mer_rlidrd.GROSS_SALES_QTY,
        mer_rlidrd.GROSS_SALES,
        mer_rlidrd.GROSS_SALES_COST,
        mer_rlidrd.GROSS_SALES_FR_COST,
        mer_rlidrd.GROSS_REG_SALES_QTY,
        mer_rlidrd.GROSS_REG_SALES,
        mer_rlidrd.GROSS_REG_SALES_COST,
        mer_rlidrd.GROSS_REG_SALES_FR_COST,
        mer_rlidrd.SDN_IN_QTY,
        mer_rlidrd.SDN_IN_SELLING,
        mer_rlidrd.SDN_IN_COST,
        mer_rlidrd.SDN_IN_FR_COST,
        mer_rlidrd.SDN_IN_CASES,
        mer_rlidrd.ACTL_STORE_RCPT_QTY,
        mer_rlidrd.ACTL_STORE_RCPT_SELLING,
        mer_rlidrd.ACTL_STORE_RCPT_COST,
        mer_rlidrd.ACTL_STORE_RCPT_FR_COST,
        mer_rlidrd.STORE_DELIV_SELLING,
        mer_rlidrd.STORE_DELIV_COST,
        mer_rlidrd.STORE_DELIV_FR_COST,
        mer_rlidrd.STORE_INTAKE_QTY,
        mer_rlidrd.STORE_INTAKE_SELLING,
        mer_rlidrd.STORE_INTAKE_COST,
        mer_rlidrd.STORE_INTAKE_FR_COST,
        mer_rlidrd.STORE_INTAKE_MARGIN,
        mer_rlidrd.SALES_RETURNS_QTY,
        mer_rlidrd.SALES_RETURNS_SELLING,
        mer_rlidrd.SALES_RETURNS_COST,
        mer_rlidrd.SALES_RETURNS_FR_COST,
        mer_rlidrd.REG_SALES_RETURNS_QTY,
        mer_rlidrd.REG_SALES_RETURNS_SELLING,
        mer_rlidrd.REG_SALES_RETURNS_COST,
        mer_rlidrd.REG_SALES_RETURNS_FR_COST,
        mer_rlidrd.CLEAR_SALES_RETURNS_SELLING,
        mer_rlidrd.CLEAR_SALES_RETURNS_COST,
        mer_rlidrd.CLEAR_SALES_RETURNS_FR_COST,
        mer_rlidrd.CLEAR_SALES_RETURNS_QTY,
        mer_rlidrd.LAST_UPDATED_DATE,
        mer_rlidrd.STORE_DELIV_QTY,
        mer_rlidrd.STORE_DELIV_CASES
);

   g_recs_inserted := g_recs_inserted + a_tbl_merge.count;
   g_recs_updated  := g_recs_updated  + sql%rowcount;

   exception
      when others then
      l_text := '- abort--4--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_text := 'g_erro_count - '||sql%bulk_exceptions.count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       g_error_count := sql%bulk_exceptions.count;
                 l_text := 'g_erro_count - '||g_error_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
l_text := '- abort--5--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_merge(g_error_index).sk1_location_no||
                       ' '||a_tbl_merge(g_error_index).sk1_item_no||
                       ' '||a_tbl_merge(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
l_text := '- abort--6--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--7--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
end local_bulk_merge;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   a_count_m               := a_count_m + 1;
   a_tbl_merge(a_count_m) := g_rec_out;
   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_merge;
      a_tbl_merge  := a_empty_set_m;
      a_count_m     := 0;
      a_count       := 0;
      commit;
   end if;

   exception
      when dwh_errors.e_insert_error then
l_text := '- abort--8--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
l_text := '- abort--9--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort-10---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      when others then
l_text := '- abort--11--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
l_text := '- abort--12--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--13--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
end local_write_output;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    
    g_forall_limit := 10000;
    
--    l_text := 'ARRAY LIMIT - '||g_forall_limit;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_LOC_ITEM_DY_RMS_DENSE EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--    begin
--    execute immediate '  alter session set events ''10046 trace name context forever, level 12''   ';
--    end;
    
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
/*    
    if p_from_loc_no = 0 then
       g_from_loc_no := 0;
       g_to_loc_no   := 400;
    end if;
    
    if p_from_loc_no  = 351 then
       g_from_loc_no := 401;
       g_to_loc_no   := 900;
    end if;
    
    if p_from_loc_no  = 491 then
       g_from_loc_no := 901;
       g_to_loc_no   := 99999;
    end if;
*/    
    if p_from_loc_no = 0 then
       g_from_loc_no := 0;
       g_to_loc_no   := 99999;
    end if;
    
    if p_from_loc_no  = 351 then
       g_from_loc_no := 0;
       g_to_loc_no   := 0;
    end if;
    
    if p_from_loc_no  = 491 then
       g_from_loc_no := 0;
       g_to_loc_no   := 0;
    end if;

--    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
  if g_to_loc_no   <> 0
  then 
  
  
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_dy_rms_sale;
    fetch c_fnd_rtl_loc_item_dy_rms_sale bulk collect into a_stg_input limit g_forall_limit;
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

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_loc_item_dy_rms_sale bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_dy_rms_sale;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_merge;
    
  end if;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
l_text := '- abort-14---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
l_text := '- abort--15--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--16--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      when others then
l_text := '- abort-17---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
l_text := '- abort-18---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       rollback;
       p_success := false;
l_text := '- abort-19---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--20--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
end wh_prf_corp_110_FIX;
