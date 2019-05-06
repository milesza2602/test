--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_130U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_130U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2008
--  Author:      Alastair de Wet
--  Purpose:     Create JV/Waste recov/WW Card fact table in the performance layer
--               with input ex POS JV table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_pos_jv
--               Output - rtl_loc_item_dy_pos_jv
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  18 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_dy_pos_jv%rowtype;

g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_130U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE POS JV/WWCARD/WASTE RECOV FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_m is table of rtl_loc_item_dy_pos_jv%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_pos_jv%rowtype index by binary_integer;
a_tbl_merge        tbl_array_m;
a_empty_set_m       tbl_array_m;

a_count             integer       := 0;
a_count_m           integer       := 0;

/* ORIGINAL SELECT CHANGED BECAUSE PRODUCT CONVERT FOR REF ITEMS WAS CAUSING DUPLICATES AND DATA WAS BEING LOST
cursor c_fnd_rtl_loc_item_dy_pos_jv is
   select pos.*,
          di.department_no,di.tran_ind,di.sk1_item_no,
          dd.jv_dept_ind,dd.packaging_dept_ind,dd.non_merch_dept_ind,
          dl.sk1_location_no,
          dih.sk2_item_no,
          dlh.sk2_location_no
   from   fnd_rtl_loc_item_dy_pos_jv pos,
          dim_item di,
          dim_department dd,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh
   where  pos.last_updated_date      = g_date and
          pos.item_no                = di.item_no  and
          di.department_no           = dd.department_no and
          pos.location_no            = dl.location_no and
          pos.item_no                = dih.item_no and
          pos.post_date              between dih.sk2_active_from_date and dih.sk2_active_to_date and
          pos.location_no            = dlh.location_no and
          pos.post_date              between dlh.sk2_active_from_date and dlh.sk2_active_to_date
          ;
*/

cursor c_fnd_rtl_loc_item_dy_pos_jv is
    with sdpt as
   (select  location_no,
            item_no,
            post_date,
            sum(spec_dept_qty) spec_dept_qty,
            sum(spec_dept_revenue) spec_dept_revenue,
            sum(waste_recov_total_qty) waste_recov_total_qty,
            sum(waste_recov_total_revenue) waste_recov_total_revenue,
            sum(wwcard_total_sales_incl_vat) wwcard_total_sales_incl_vat,
            sum(waste_rcov_ww_crd_rvnu_inc_vat) waste_rcov_ww_crd_rvnu_inc_vat
   from
   ( select location_no,
            dwh_performance.dwh_lookup.item_convert(pos.item_no) as item_no,
            post_date,
            spec_dept_qty,
            spec_dept_revenue,
            waste_recov_total_qty,
            waste_recov_total_revenue,
            wwcard_total_sales_incl_vat,
            waste_rcov_ww_crd_rvnu_inc_vat
      from  fnd_rtl_loc_item_dy_pos_jv pos
      where last_updated_date  = g_date ) lidlist

      group by lidlist.location_no,lidlist.item_no,lidlist.post_date)

   select sdpt.*,
          di.department_no,di.tran_ind,di.sk1_item_no,
          dd.jv_dept_ind,dd.packaging_dept_ind,dd.non_merch_dept_ind,
          dl.sk1_location_no,
          dih.sk2_item_no,
          dlh.sk2_location_no
   from   sdpt,
          dim_item di,
          dim_department dd,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh
   where  sdpt.item_no               = di.item_no  and
          di.department_no           = dd.department_no and
          sdpt.location_no           = dl.location_no and
          sdpt.item_no               = dih.item_no and
          sdpt.post_date             between dih.sk2_active_from_date and dih.sk2_active_to_date and
          sdpt.location_no           = dlh.location_no and
          sdpt.post_date             between dlh.sk2_active_from_date and dlh.sk2_active_to_date
          ;

g_rec_in             c_fnd_rtl_loc_item_dy_pos_jv%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_dy_pos_jv%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.spec_dept_qty                   := g_rec_in.spec_dept_qty;
   g_rec_out.spec_dept_revenue               := g_rec_in.spec_dept_revenue;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;

-- Convert Items not at RMS transaction level to RMS transaction level
--   if g_rec_in.tran_ind = 0 then
--      dwh_lookup.dim_item_convert(g_rec_in.item_no,g_rec_in.item_no);
-- Look up the Surrogate keys from dimensions for output onto the Fact record
--      dwh_lookup.dim_item_sk1(g_rec_in.item_no,g_rec_out.sk1_item_no);
--      dwh_lookup.dim_item_sk2(g_rec_in.item_no,g_rec_out.sk2_item_no);
--  end if;

-- Value add and calculated fields added to performance layer

   g_rec_out.waste_recov_qty                 := '';
   g_rec_out.waste_recov_revenue             := '';
   g_rec_out.waste_rcov_ww_crd_rvnu_inc_vat  := '';
   g_rec_out.wwcard_sales_incl_vat           := '';
   g_rec_out.spec_dept_waste_recov_qty       := '';
   g_rec_out.spec_dept_waste_recov_revenue   := '';
   g_rec_out.spec_dept_ww_card_rvnu          := '';
   g_rec_out.spec_dept_wst_rcov_ww_crd_rvnu  := '';
   g_rec_out.non_merch_ww_card_sales         := '';


   if g_rec_in.jv_dept_ind        <> 1 and
      g_rec_in.packaging_dept_ind <> 1 then
      g_rec_out.waste_recov_qty                    := g_rec_in.waste_recov_total_qty;
      g_rec_out.waste_recov_revenue                := g_rec_in.waste_recov_total_revenue;
      g_rec_out.waste_rcov_ww_crd_rvnu_inc_vat     := g_rec_in.waste_rcov_ww_crd_rvnu_inc_vat;
      if g_rec_in.non_merch_dept_ind  <> 1 then
         g_rec_out.wwcard_sales_incl_vat           := g_rec_in.wwcard_total_sales_incl_vat;
      end if;
   end if;

   if g_rec_in.jv_dept_ind        = 1 or
      g_rec_in.packaging_dept_ind = 1 then
      g_rec_out.spec_dept_waste_recov_qty          := g_rec_in.waste_recov_total_qty;
      g_rec_out.spec_dept_waste_recov_revenue      := g_rec_in.waste_recov_total_revenue;
      g_rec_out.spec_dept_ww_card_rvnu             := g_rec_in.wwcard_total_sales_incl_vat;
      g_rec_out.spec_dept_wst_rcov_ww_crd_rvnu     := g_rec_in.waste_rcov_ww_crd_rvnu_inc_vat;
   end if;

   if g_rec_in.jv_dept_ind        = 1 or
      g_rec_in.packaging_dept_ind = 1 or
      g_rec_in.non_merch_dept_ind = 1 then
      g_rec_out.non_merch_ww_card_sales             := g_rec_in.wwcard_total_sales_incl_vat;
   end if;


   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions

MERGE INTO rtl_loc_item_dy_pos_jv rtl_lidpj
USING
(SELECT
         a_tbl_merge(i).SK1_LOCATION_NO                 as  SK1_LOCATION_NO,
         a_tbl_merge(i).SK1_ITEM_NO                     as  SK1_ITEM_NO,
         a_tbl_merge(i).POST_DATE                       as  POST_DATE,
         a_tbl_merge(i).SK2_LOCATION_NO                 as  SK2_LOCATION_NO,
         a_tbl_merge(i).SK2_ITEM_NO                     as  SK2_ITEM_NO,
         a_tbl_merge(i).WASTE_RECOV_QTY                 as  WASTE_RECOV_QTY,
         a_tbl_merge(i).WASTE_RECOV_REVENUE             as  WASTE_RECOV_REVENUE,
         a_tbl_merge(i).SPEC_DEPT_QTY                   as  SPEC_DEPT_QTY,
         a_tbl_merge(i).SPEC_DEPT_REVENUE               as  SPEC_DEPT_REVENUE,
         a_tbl_merge(i).SPEC_DEPT_WASTE_RECOV_QTY       as  SPEC_DEPT_WASTE_RECOV_QTY,
         a_tbl_merge(i).SPEC_DEPT_WASTE_RECOV_REVENUE   as  SPEC_DEPT_WASTE_RECOV_REVENUE,
         a_tbl_merge(i).WWCARD_SALES_INCL_VAT           as  WWCARD_SALES_INCL_VAT,
         a_tbl_merge(i).NON_MERCH_WW_CARD_SALES         as  NON_MERCH_WW_CARD_SALES,
         a_tbl_merge(i).SPEC_DEPT_WW_CARD_RVNU          as  SPEC_DEPT_WW_CARD_RVNU,
         a_tbl_merge(i).WASTE_RCOV_WW_CRD_RVNU_INC_VAT  as  WASTE_RCOV_WW_CRD_RVNU_INC_VAT,
         a_tbl_merge(i).SPEC_DEPT_WST_RCOV_WW_CRD_RVNU  as  SPEC_DEPT_WST_RCOV_WW_CRD_RVNU,
         a_tbl_merge(i).LAST_UPDATED_DATE               as  LAST_UPDATED_DATE
FROM dual) mer_lidpj
ON (rtl_lidpj.SK1_LOCATION_NO = mer_lidpj.SK1_LOCATION_NO
AND rtl_lidpj.SK1_ITEM_NO = mer_lidpj.SK1_ITEM_NO
AND rtl_lidpj.POST_DATE = mer_lidpj.POST_DATE)
WHEN MATCHED THEN
UPDATE
SET      waste_recov_qty                 = mer_lidpj.waste_recov_qty,
         waste_recov_revenue             = mer_lidpj.waste_recov_revenue,
         spec_dept_qty                   = mer_lidpj.spec_dept_qty,
         spec_dept_revenue               = mer_lidpj.spec_dept_revenue,
         spec_dept_waste_recov_qty       = mer_lidpj.spec_dept_waste_recov_qty,
         spec_dept_waste_recov_revenue   = mer_lidpj.spec_dept_waste_recov_revenue,
         wwcard_sales_incl_vat           = mer_lidpj.wwcard_sales_incl_vat,
         non_merch_ww_card_sales         = mer_lidpj.non_merch_ww_card_sales,
         spec_dept_ww_card_rvnu          = mer_lidpj.spec_dept_ww_card_rvnu,
         waste_rcov_ww_crd_rvnu_inc_vat  = mer_lidpj.waste_rcov_ww_crd_rvnu_inc_vat,
         spec_dept_wst_rcov_ww_crd_rvnu  = mer_lidpj.spec_dept_wst_rcov_ww_crd_rvnu,
         last_updated_date               = mer_lidpj.last_updated_date
WHEN NOT MATCHED THEN
INSERT
(
         rtl_lidpj.SK1_LOCATION_NO,
         rtl_lidpj.SK1_ITEM_NO,
         rtl_lidpj.POST_DATE,
         rtl_lidpj.SK2_LOCATION_NO,
         rtl_lidpj.SK2_ITEM_NO,
         rtl_lidpj.WASTE_RECOV_QTY,
         rtl_lidpj.WASTE_RECOV_REVENUE,
         rtl_lidpj.SPEC_DEPT_QTY,
         rtl_lidpj.SPEC_DEPT_REVENUE,
         rtl_lidpj.SPEC_DEPT_WASTE_RECOV_QTY,
         rtl_lidpj.SPEC_DEPT_WASTE_RECOV_REVENUE,
         rtl_lidpj.WWCARD_SALES_INCL_VAT,
         rtl_lidpj.NON_MERCH_WW_CARD_SALES,
         rtl_lidpj.SPEC_DEPT_WW_CARD_RVNU,
         rtl_lidpj.WASTE_RCOV_WW_CRD_RVNU_INC_VAT,
         rtl_lidpj.SPEC_DEPT_WST_RCOV_WW_CRD_RVNU,
         rtl_lidpj.LAST_UPDATED_DATE

)
VALUES
(
         mer_lidpj.SK1_LOCATION_NO,
         mer_lidpj.SK1_ITEM_NO,
         mer_lidpj.POST_DATE,
         mer_lidpj.SK2_LOCATION_NO,
         mer_lidpj.SK2_ITEM_NO,
         mer_lidpj.WASTE_RECOV_QTY,
         mer_lidpj.WASTE_RECOV_REVENUE,
         mer_lidpj.SPEC_DEPT_QTY,
         mer_lidpj.SPEC_DEPT_REVENUE,
         mer_lidpj.SPEC_DEPT_WASTE_RECOV_QTY,
         mer_lidpj.SPEC_DEPT_WASTE_RECOV_REVENUE,
         mer_lidpj.WWCARD_SALES_INCL_VAT,
         mer_lidpj.NON_MERCH_WW_CARD_SALES,
         mer_lidpj.SPEC_DEPT_WW_CARD_RVNU,
         mer_lidpj.WASTE_RCOV_WW_CRD_RVNU_INC_VAT,
         mer_lidpj.SPEC_DEPT_WST_RCOV_WW_CRD_RVNU,
         mer_lidpj.LAST_UPDATED_DATE
);

    g_recs_inserted := g_recs_inserted + a_tbl_merge.count;

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
                       ' '||a_tbl_merge(g_error_index).sk1_location_no||
                       ' '||a_tbl_merge(g_error_index).sk1_item_no||
                       ' '||a_tbl_merge(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

-- Place data into and array for later writing to table in bulk
   a_count_m               := a_count_m + 1;
   a_tbl_merge(a_count_m) := g_rec_out;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;



      a_tbl_merge  := a_empty_set_m;
      a_count_m     := 0;
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

    l_text := 'LOAD OF RTL_LOC_ITEM_DY_POS_JV EX FOUNDATION STARTED AT '||
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
    open c_fnd_rtl_loc_item_dy_pos_jv;
    fetch c_fnd_rtl_loc_item_dy_pos_jv bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_loc_item_dy_pos_jv bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_dy_pos_jv;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;

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
end wh_prf_corp_130u;
