--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_114U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_114U_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        FEB 2009
--  Author:      Alastair de Wet
--  Purpose:     Create Trunked measures store rcpt fact table in the performance layer
--               with input ex RMS Shipment  table from foundation layer.
--  Tables:      Input  - fnd_rtl_shipment
--               Output - rtl_loc_item_dy_rms_sparse
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  18 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
--
--  29 april 2015 wendy lyttle  DAVID JONES - do not load where  chain_code = 'DJ'
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
g_rec_out            rtl_loc_item_dy_rms_sparse%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_114U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS SHIPMENT DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_m is table of rtl_loc_item_dy_rms_sparse%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_rms_sparse%rowtype index by binary_integer;
a_tbl_merge         tbl_array_m;
a_empty_set_m       tbl_array_m;


a_count             integer       := 0;
a_count_m           integer       := 0;

--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- 'With' creates a sub query which is treated as a table called 'lid_list' and used in the from clause of the main query.
-- This option is known as subquery factoring and eliminates the need to create a temp table of the 1st result set.
--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
cursor c_fnd_rtl_shipment is
   with lid_list as
   (
   select item_no,to_loc_no,actl_rcpt_date
   from   fnd_rtl_shipment shp,
          dim_location dl1,
          dim_location dl2
   where  shp.last_updated_date  = g_date and
          (shp.to_loc_no          = dl1.location_no and dl1.loc_type            = 'W') and
          (shp.from_loc_no        = dl2.location_no and dl2.loc_type            = 'W') and
          shp.actl_rcpt_date     is not null
          AND (CHAIN_CODE <> 'DJ' or chain_code is null)
   --       AND dl1.item_no = 6008000436045 and shp.to_loc_no = 400 and shp.actl_rcpt_date = '29/JUL/15'
   group by item_no,to_loc_no, shp.actl_rcpt_date
   )
   select sum(nvl(shp.sdn_qty,0) * (shp.reg_rsp * 100 / (100 + di.vat_rate_perc))) as trunked_selling,
          sum(nvl(shp.sdn_qty,0) * shp.cost_price) as trunked_cost,
          shp.actl_rcpt_date,
          di.sk1_item_no,
          dl.sk1_location_no,
    --      max(nvl(fnd_li.num_units_per_tray,1)) as num_units_per_tray,
          max(di.business_unit_no) as business_unit_no,
          max(di.random_mass_ind) as random_mass_ind,
        max(di.standard_uom_code) as standard_uom_code,
          max(nvl(di.static_mass,1)) as static_mass,
   --       max(nvl(dd.gifting_dept_ind,0)) as gifting_department_ind,
   --       max(nvl(dd.book_magazine_dept_ind,0)) as book_magazine_dept_ind, 
          max(dlh.sk2_location_no) as sk2_location_no,
          max(dih.sk2_item_no) as sk2_item_no
    
   from   fnd_rtl_shipment shp,
          lid_list ,
          dim_item di,
          dim_item_hist dih,
          fnd_location_item fnd_li,
          dim_location dl,
          dim_location_hist dlh,
          dim_department dd
   where  shp.item_no                = lid_list.item_no        and
          shp.to_loc_no              = lid_list.to_loc_no      and
          shp.actl_rcpt_date         = lid_list.actl_rcpt_date  and
          shp.sdn_qty           <> 0                  and
          shp.sdn_qty           is not null           and
          lid_list.item_no                = di.item_no          and
          lid_list.item_no                = dih.item_no         and
          lid_list.actl_rcpt_date         between dih.sk2_active_from_date and dih.sk2_active_to_date and
          lid_list.to_loc_no              = dl.location_no      and
          lid_list.to_loc_no              = dlh.location_no     and
          lid_list.actl_rcpt_date         between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
          lid_list.item_no                = fnd_li.item_no(+) and
          lid_list.to_loc_no              = fnd_li.location_no(+) and
          di.department_no           = dd.department_no
   group by shp.actl_rcpt_date, di.sk1_item_no, dl.sk1_location_no;



g_rec_in             c_fnd_rtl_shipment%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_shipment%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin


   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.post_date                       := g_rec_in.actl_rcpt_date;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
 --  g_rec_out.trunked_qty                     := g_rec_in.trunked_qty;
   g_rec_out.trunked_cost                    := g_rec_in.trunked_cost;        
   g_rec_out.trunked_selling                 := g_rec_in.trunked_selling;     
 --  g_rec_out.last_updated_date               := g_date;

 --  if g_rec_in.business_unit_no = 50 then
 --     g_rec_out.trunked_cases                := round((nvl(g_rec_in.trunked_qty,0)/g_rec_in.num_units_per_tray),0);
 --  else
 --     g_rec_out.trunked_cases                := null;
 --  end if;
   if g_rec_in.business_unit_no = 50 and  g_rec_in.standard_uom_code = 'EA' and g_rec_in.random_mass_ind = 1 then
      g_rec_out.trunked_cost                 := g_rec_out.trunked_cost * g_rec_in.static_mass;
      g_rec_out.trunked_selling              := g_rec_out.trunked_selling * g_rec_in.static_mass;
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
procedure local_bulk_merge as
begin
    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions
MERGE INTO rtl_loc_item_dy_rms_sparse rtl_lidrs
USING
(SELECT  a_tbl_merge(i).SK1_LOCATION_NO                 AS    SK1_LOCATION_NO,
         a_tbl_merge(i).SK1_ITEM_NO                     AS    SK1_ITEM_NO,
         a_tbl_merge(i).POST_DATE                       AS    POST_DATE,
         a_tbl_merge(i).SK2_LOCATION_NO                 AS    SK2_LOCATION_NO,
         a_tbl_merge(i).SK2_ITEM_NO                     AS    SK2_ITEM_NO,
         a_tbl_merge(i).PROM_SALES_QTY                  AS    PROM_SALES_QTY,
         a_tbl_merge(i).PROM_SALES                      AS    PROM_SALES,
         a_tbl_merge(i).PROM_SALES_COST                 AS    PROM_SALES_COST,
         a_tbl_merge(i).PROM_SALES_FR_COST              AS    PROM_SALES_FR_COST,
         a_tbl_merge(i).PROM_SALES_MARGIN               AS    PROM_SALES_MARGIN,
         a_tbl_merge(i).FRANCHISE_PROM_SALES            AS    FRANCHISE_PROM_SALES,
         a_tbl_merge(i).FRANCHISE_PROM_SALES_MARGIN     AS    FRANCHISE_PROM_SALES_MARGIN,
         a_tbl_merge(i).PROM_DISCOUNT_NO                AS    PROM_DISCOUNT_NO,
         a_tbl_merge(i).HO_PROM_DISCOUNT_AMT            AS    HO_PROM_DISCOUNT_AMT,
         a_tbl_merge(i).HO_PROM_DISCOUNT_QTY            AS    HO_PROM_DISCOUNT_QTY,
         a_tbl_merge(i).ST_PROM_DISCOUNT_AMT            AS    ST_PROM_DISCOUNT_AMT,
         a_tbl_merge(i).ST_PROM_DISCOUNT_QTY            AS    ST_PROM_DISCOUNT_QTY,
         a_tbl_merge(i).CLEAR_SALES_QTY                 AS    CLEAR_SALES_QTY,
         a_tbl_merge(i).CLEAR_SALES                     AS    CLEAR_SALES,
         a_tbl_merge(i).CLEAR_SALES_COST                AS    CLEAR_SALES_COST,
         a_tbl_merge(i).CLEAR_SALES_FR_COST             AS    CLEAR_SALES_FR_COST,
         a_tbl_merge(i).CLEAR_SALES_MARGIN              AS    CLEAR_SALES_MARGIN,
         a_tbl_merge(i).FRANCHISE_CLEAR_SALES           AS    FRANCHISE_CLEAR_SALES,
         a_tbl_merge(i).FRANCHISE_CLEAR_SALES_MARGIN    AS    FRANCHISE_CLEAR_SALES_MARGIN,
         a_tbl_merge(i).WASTE_QTY                       AS    WASTE_QTY,
         a_tbl_merge(i).WASTE_SELLING                   AS    WASTE_SELLING,
         a_tbl_merge(i).WASTE_COST                      AS    WASTE_COST,
         a_tbl_merge(i).WASTE_FR_COST                   AS    WASTE_FR_COST,
         a_tbl_merge(i).SHRINK_QTY                      AS    SHRINK_QTY,
         a_tbl_merge(i).SHRINK_SELLING                  AS    SHRINK_SELLING,
         a_tbl_merge(i).SHRINK_COST                     AS    SHRINK_COST,
         a_tbl_merge(i).SHRINK_FR_COST                  AS    SHRINK_FR_COST,
         a_tbl_merge(i).GAIN_QTY                        AS    GAIN_QTY,
         a_tbl_merge(i).GAIN_SELLING                    AS    GAIN_SELLING,
         a_tbl_merge(i).GAIN_COST                       AS    GAIN_COST,
         a_tbl_merge(i).GAIN_FR_COST                    AS    GAIN_FR_COST,
         a_tbl_merge(i).GRN_QTY                         AS    GRN_QTY,
         a_tbl_merge(i).GRN_CASES                       AS    GRN_CASES,
         a_tbl_merge(i).GRN_SELLING                     AS    GRN_SELLING,
         a_tbl_merge(i).GRN_COST                        AS    GRN_COST,
         a_tbl_merge(i).GRN_FR_COST                     AS    GRN_FR_COST,
         a_tbl_merge(i).GRN_MARGIN                      AS    GRN_MARGIN,
         a_tbl_merge(i).SHRINKAGE_QTY                   AS    SHRINKAGE_QTY,
         a_tbl_merge(i).SHRINKAGE_SELLING               AS    SHRINKAGE_SELLING,
         a_tbl_merge(i).SHRINKAGE_COST                  AS    SHRINKAGE_COST,
         a_tbl_merge(i).SHRINKAGE_FR_COST               AS    SHRINKAGE_FR_COST,
         a_tbl_merge(i).ABS_SHRINKAGE_QTY               AS    ABS_SHRINKAGE_QTY,
         a_tbl_merge(i).ABS_SHRINKAGE_SELLING           AS    ABS_SHRINKAGE_SELLING,
         a_tbl_merge(i).ABS_SHRINKAGE_COST              AS    ABS_SHRINKAGE_COST,
         a_tbl_merge(i).ABS_SHRINKAGE_FR_COST           AS    ABS_SHRINKAGE_FR_COST,
         a_tbl_merge(i).CLAIM_QTY                       AS    CLAIM_QTY,
         a_tbl_merge(i).CLAIM_SELLING                   AS    CLAIM_SELLING,
         a_tbl_merge(i).CLAIM_COST                      AS    CLAIM_COST,
         a_tbl_merge(i).CLAIM_FR_COST                   AS    CLAIM_FR_COST,
         a_tbl_merge(i).SELF_SUPPLY_QTY                 AS    SELF_SUPPLY_QTY,
         a_tbl_merge(i).SELF_SUPPLY_SELLING             AS    SELF_SUPPLY_SELLING,
         a_tbl_merge(i).SELF_SUPPLY_COST                AS    SELF_SUPPLY_COST,
         a_tbl_merge(i).SELF_SUPPLY_FR_COST             AS    SELF_SUPPLY_FR_COST,
         a_tbl_merge(i).WAC_ADJ_AMT                     AS    WAC_ADJ_AMT,
         a_tbl_merge(i).INVOICE_ADJ_QTY                 AS    INVOICE_ADJ_QTY,
         a_tbl_merge(i).INVOICE_ADJ_SELLING             AS    INVOICE_ADJ_SELLING,
         a_tbl_merge(i).INVOICE_ADJ_COST                AS    INVOICE_ADJ_COST,
         a_tbl_merge(i).RNDM_MASS_POS_VAR               AS    RNDM_MASS_POS_VAR,
         a_tbl_merge(i).MKUP_SELLING                    AS    MKUP_SELLING,
         a_tbl_merge(i).MKUP_CANCEL_SELLING             AS    MKUP_CANCEL_SELLING,
         a_tbl_merge(i).MKDN_SELLING                    AS    MKDN_SELLING,
         a_tbl_merge(i).MKDN_CANCEL_SELLING             AS    MKDN_CANCEL_SELLING,
         a_tbl_merge(i).PROM_MKDN_QTY                   AS    PROM_MKDN_QTY,
         a_tbl_merge(i).PROM_MKDN_SELLING               AS    PROM_MKDN_SELLING,
         a_tbl_merge(i).CLEAR_MKDN_SELLING              AS    CLEAR_MKDN_SELLING,
         a_tbl_merge(i).MKDN_SALES_QTY                  AS    MKDN_SALES_QTY,
         a_tbl_merge(i).MKDN_SALES                      AS    MKDN_SALES,
         a_tbl_merge(i).MKDN_SALES_COST                 AS    MKDN_SALES_COST,
         a_tbl_merge(i).NET_MKDN                        AS    NET_MKDN,
         a_tbl_merge(i).RTV_QTY                         AS    RTV_QTY,
         a_tbl_merge(i).RTV_CASES                       AS    RTV_CASES,
         a_tbl_merge(i).RTV_SELLING                     AS    RTV_SELLING,
         a_tbl_merge(i).RTV_COST                        AS    RTV_COST,
         a_tbl_merge(i).RTV_FR_COST                     AS    RTV_FR_COST,
         a_tbl_merge(i).SDN_OUT_QTY                     AS    SDN_OUT_QTY,
         a_tbl_merge(i).SDN_OUT_SELLING                 AS    SDN_OUT_SELLING,
         a_tbl_merge(i).SDN_OUT_COST                    AS    SDN_OUT_COST,
         a_tbl_merge(i).SDN_OUT_FR_COST                 AS    SDN_OUT_FR_COST,
         a_tbl_merge(i).SDN_OUT_CASES                   AS    SDN_OUT_CASES,
         a_tbl_merge(i).IBT_IN_QTY                      AS    IBT_IN_QTY,
         a_tbl_merge(i).IBT_IN_SELLING                  AS    IBT_IN_SELLING,
         a_tbl_merge(i).IBT_IN_COST                     AS    IBT_IN_COST,
         a_tbl_merge(i).IBT_IN_FR_COST                  AS    IBT_IN_FR_COST,
         a_tbl_merge(i).IBT_OUT_QTY                     AS    IBT_OUT_QTY,
         a_tbl_merge(i).IBT_OUT_SELLING                 AS    IBT_OUT_SELLING,
         a_tbl_merge(i).IBT_OUT_COST                    AS    IBT_OUT_COST,
         a_tbl_merge(i).IBT_OUT_FR_COST                 AS    IBT_OUT_FR_COST,
         a_tbl_merge(i).NET_IBT_QTY                     AS    NET_IBT_QTY,
         a_tbl_merge(i).NET_IBT_SELLING                 AS    NET_IBT_SELLING,
         a_tbl_merge(i).SHRINK_EXCL_SOME_DEPT_COST      AS    SHRINK_EXCL_SOME_DEPT_COST,
         a_tbl_merge(i).GAIN_EXCL_SOME_DEPT_COST        AS    GAIN_EXCL_SOME_DEPT_COST,
         a_tbl_merge(i).NET_WASTE_QTY                   AS    NET_WASTE_QTY,
         a_tbl_merge(i).TRUNKED_QTY                     AS    TRUNKED_QTY,
         a_tbl_merge(i).TRUNKED_CASES                   AS    TRUNKED_CASES,
         a_tbl_merge(i).TRUNKED_SELLING                 AS    TRUNKED_SELLING,
         a_tbl_merge(i).TRUNKED_COST                    AS    TRUNKED_COST,
         a_tbl_merge(i).DC_DELIVERED_QTY                AS    DC_DELIVERED_QTY,
         a_tbl_merge(i).DC_DELIVERED_CASES              AS    DC_DELIVERED_CASES,
         a_tbl_merge(i).DC_DELIVERED_SELLING            AS    DC_DELIVERED_SELLING,
         a_tbl_merge(i).DC_DELIVERED_COST               AS    DC_DELIVERED_COST,
         a_tbl_merge(i).NET_INV_ADJ_QTY                 AS    NET_INV_ADJ_QTY,
         a_tbl_merge(i).NET_INV_ADJ_SELLING             AS    NET_INV_ADJ_SELLING,
         a_tbl_merge(i).NET_INV_ADJ_COST                AS    NET_INV_ADJ_COST,
         a_tbl_merge(i).NET_INV_ADJ_FR_COST             AS    NET_INV_ADJ_FR_COST,
         a_tbl_merge(i).LAST_UPDATED_DATE               AS    LAST_UPDATED_DATE,
         a_tbl_merge(i).CH_ALLOC_QTY                    AS    CH_ALLOC_QTY,
         a_tbl_merge(i).CH_ALLOC_SELLING                AS    CH_ALLOC_SELLING
FROM dual
) mer_lidrs
ON (rtl_lidrs.SK1_LOCATION_NO = mer_lidrs.SK1_LOCATION_NO
AND rtl_lidrs.SK1_ITEM_NO = mer_lidrs.SK1_ITEM_NO
AND rtl_lidrs.POST_DATE = mer_lidrs.POST_DATE)
WHEN MATCHED THEN
UPDATE SET   trunked_selling         = mer_lidrs.trunked_selling
           --  trunked_cost            = mer_lidrs.trunked_cost
           --  last_updated_date       = mer_lidrs.last_updated_date

         ;

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
end local_bulk_merge;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
      a_count_m              := a_count_m + 1;
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

    l_text := 'LOAD OF rtl_loc_item_dy_rms_sparse EX FOUNDATION STARTED AT (** VAT FIX **)'||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '15/JUL/15'; -- testing only
    
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
 while g_date < '10/AUG/15' loop
 
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    open c_fnd_rtl_shipment;
    fetch c_fnd_rtl_shipment bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_shipment bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_shipment;
    
    g_date := g_date + 1;
    
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_merge;

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
    
end loop;

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
end wh_prf_corp_114u_fix;