--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_052U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_052U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        June 2013    Rewritten for Foods Renewal
--  Author:      Alastair de Wet
--  Purpose:     Create Zone Item OM fact table in the performance layer
--               with input ex JDA fnd_zone_item table from foundation layer.
--
--  Tables:      Input  - fnd_zone_item
--               Output - rtl_zone_item_om
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  04 Jun 2016 - B kirschner:                                                                                  Ref: BK04Jun16
--                Add update to col NUM_EXTRA_LEADTIME_DAYS in output table. 
--                To get this col need to replace cursor to include join with table fnd_zone_item_supp on 
--                ITEM_NO||supplier_no||ZONE_NO in order to get required col.
--                NB: SP WH_PRF_CORP_081U needs to complete first in order to populate NUM_EXTRA_LEADTIME_DAYS.
--  22 Aug 2016 - B Kirschner:                                                                                  Ref: BK22Aug16
--                Include col FROM_LOC_NO from source as above with join with table fnd_zone_item_supp
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria
--  20 Sep 2016 - A Joshua xxx -- Cater for ETL for measures reg_rsp_excl_vat and cost_price                Ref: AJ-chgxxx

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
g_rec_out            rtl_zone_item_om%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_052U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE ITEM OM FACTS EX AMOS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_om%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_om%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
g_vat_rate_perc      dim_item.vat_rate_perc%type;

-- BK04Jun16 (start)
cursor c_fnd_zone_item is
  with PRTA as 
    (
     select   fiz.*,
              di.sk1_item_no,
              dz.sk1_zone_group_zone_no,
              di.SK1_SUPPLIER_NO,
              fiz.item_no||fiz.supplier_no||fiz.zone_no as pkey
       from   fnd_zone_item fiz,
              dim_item di,
              dim_zone dz
--              fnd_jdaff_dept_rollout dept
       where  fiz.item_no             = di.item_no  and
              fiz.zone_no             = dz.zone_no and
              fiz.zone_group_no       = dz.zone_group_no and
              di.business_unit_no     = 50
--              di.department_no        = dept.department_no and
--              dept.department_live_ind = 'Y'
    order by 3,2
  ),

   prtB as 
  (
    select  ITEM_NO||supplier_no||ZONE_NO as pkey,
            NUM_EXTRA_LEAD_TIME_DAYS,
            FROM_LOC_NO,                                                        -- BK22Aug16
            LAST_UPDATED_DATE                                                   -- BK22Aug16
    from    DWH_FOUNDATION.fnd_zone_item_supp zis 
--    where   exists (select 1 from prtA a where a.pkey = zis.ITEM_NO||zis.supplier_no||zis.ZONE_NO)
  )
  
  select   ZONE_GROUP_NO
          ,ZONE_NO
          ,ITEM_NO
          ,BASE_RETAIL_IND
          ,REG_RSP
          ,SELLING_UOM_CODE
          ,MARKET_BASKET_CODE
          ,ITEM_ZONE_LINK_CODE
          ,MULTI_SELLING_UOM_CODE
          ,SELLING_UNIT_RSP
          ,MULTI_UNIT_RSP
          ,MULTI_QTY
          ,SOURCE_DATA_STATUS_CODE
          ,a.LAST_UPDATED_DATE
          ,CASE_MASS
          ,CASE_SELLING_INCL_VAT
          ,REG_RSP_INCL_VAT
          ,CASE_COST_PRICE
          ,NUM_UNITS_PER_TRAY
          ,PRODUCT_STATUS_CODE
          ,PRODUCT_STATUS_1_CODE
          ,MU_PACK_HEIGHT_CODE
          ,NUM_SHELF_LIFE_DAYS
          ,TRAY_SIZE_CODE
          ,MU_EXTSN_SLEEVE_CODE
          ,MIN_ORDER_QTY
          ,CASE_SELLING_EXCL_VAT
          ,SUPPLIER_NO
          ,SHIP_HI
          ,SHIP_TI
          ,CURRENCY_CODE
          ,CURRENCY_REG_RSP
--Multi Currency--          
          ,CASE_COST_PRICE_LOCAL
          ,CASE_COST_PRICE_OPR
          ,CASE_SELLING_EXCL_VAT_LOCAL
          ,CASE_SELLING_EXCL_VAT_OPR
          ,CASE_SELLING_INCL_VAT_LOCAL
          ,CASE_SELLING_INCL_VAT_OPR
          ,COST_PRICE_LOCAL
          ,COST_PRICE_OPR
          ,REG_RSP_EXCL_VAT_LOCAL
          ,REG_RSP_EXCL_VAT_OPR
          ,REG_RSP_INCL_VAT_LOCAL
          ,REG_RSP_INCL_VAT_OPR
          ,REG_RSP_LOCAL
          ,REG_RSP_OPR
          ,SELLING_UNIT_RSP_LOCAL
          ,SELLING_UNIT_RSP_OPR
          ,MULTI_UNIT_RSP_LOCAL
          ,MULTI_UNIT_RSP_OPR
          
          ,sk1_item_no
          ,sk1_zone_group_zone_no
          
          ,b.NUM_EXTRA_LEAD_TIME_DAYS 
          ,b.from_loc_no                                                                 -- BK22Aug16
--          ,reg_rsp_excl_vat                                                              -- AJ-chgxxx
          ,cost_price                                                                    -- AJ-chgxxx PUT BACK NOV 2018
  from    prtA a
  left join 
          prtb b on (a.pkey = b.pkey and a.LAST_UPDATED_DATE = b.LAST_UPDATED_DATE)
  order by 3,2,1;
  -- BK04Jun16 (end)

-- order by only where sequencing is essential to the correct loading of data

-- For input bulk collect --
type stg_array is table of c_fnd_zone_item%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_zone_item%rowtype;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.reg_rsp                        := g_rec_in.reg_rsp;
   g_rec_out.case_mass                      := g_rec_in.case_mass;

   g_rec_out.mu_pack_height_code            := g_rec_in.mu_pack_height_code;
   g_rec_out.num_shelf_life_days            := g_rec_in.num_shelf_life_days;
   g_rec_out.tray_size_code                 := g_rec_in.tray_size_code;
   g_rec_out.mu_extsn_sleeve_code           := g_rec_in.mu_extsn_sleeve_code;
   g_rec_out.min_order_qty                  := g_rec_in.min_order_qty;
   g_rec_out.case_selling_excl_vat          := g_rec_in.case_selling_excl_vat;
   g_rec_out.case_selling                   := g_rec_in.case_selling_incl_vat;
   g_rec_out.case_cost                      := g_rec_in.case_cost_price;
   g_rec_out.num_units_per_tray             := g_rec_in.num_units_per_tray;
   --g_rec_out.product_status_code            := g_rec_in.product_status_code;
   --g_rec_out.product_status_1_code          := g_rec_in.product_status_1_code;
--multi currency ---  
    g_rec_out.case_cost_price_local           := g_rec_in.case_cost_price_local;
    g_rec_out.case_cost_price_opr             := g_rec_in.case_cost_price_opr;
    g_rec_out.case_selling_excl_vat_local     := g_rec_in.case_selling_excl_vat_local;
    g_rec_out.case_selling_excl_vat_opr       := g_rec_in.case_selling_excl_vat_opr;
    g_rec_out.case_selling_incl_vat_local     := g_rec_in.case_selling_incl_vat_local;
    g_rec_out.case_selling_incl_vat_opr       := g_rec_in.case_selling_incl_vat_opr;
    g_rec_out.cost_price_local                := g_rec_in.cost_price_local;
    g_rec_out.cost_price_opr                  := g_rec_in.cost_price_opr;
    g_rec_out.reg_rsp_excl_vat_local          := g_rec_in.reg_rsp_excl_vat_local;
    g_rec_out.reg_rsp_excl_vat_opr            := g_rec_in.reg_rsp_excl_vat_opr;
    g_rec_out.reg_rsp_incl_vat_local          := g_rec_in.reg_rsp_incl_vat_local;
    g_rec_out.reg_rsp_incl_vat_opr            := g_rec_in.reg_rsp_incl_vat_opr;
    g_rec_out.reg_rsp_local                   := g_rec_in.reg_rsp_local;
    g_rec_out.reg_rsp_opr                     := g_rec_in.reg_rsp_opr;
    g_rec_out.selling_unit_rsp_local          := g_rec_in.selling_unit_rsp_local;
    g_rec_out.selling_unit_rsp_opr            := g_rec_in.selling_unit_rsp_opr;
    g_rec_out.multi_unit_rsp_local            := g_rec_in.multi_unit_rsp_local;
    g_rec_out.multi_unit_rsp_opr              := g_rec_in.multi_unit_rsp_opr;


   Case
        when g_rec_in.product_status_code = 'A'
            then g_rec_out.product_status_code             :=1 ;
        when g_rec_in.product_status_code = 'D'
            then g_rec_out.product_status_code             :=4 ;
        when g_rec_in.product_status_code = 'N'
            then g_rec_out.product_status_code             :=14 ;
        when g_rec_in.product_status_code = 'O'
            then g_rec_out.product_status_code             :=15;
        when g_rec_in.product_status_code = 'U'
            then g_rec_out.product_status_code             :=21 ;
        when g_rec_in.product_status_code = 'X'
            then g_rec_out.product_status_code             :=24;
        when g_rec_in.product_status_code = 'Z'
            then g_rec_out.product_status_code             :=26;
        when g_rec_in.product_status_code is null
            then g_rec_out.product_status_code             :=0;
        else g_rec_out.product_status_code             :=0 ;
  end case;
  Case
        when g_rec_in.product_status_1_code = 'A'
            then g_rec_out.product_status_1_code             :=1 ;
        when g_rec_in.product_status_1_code = 'D'
            then g_rec_out.product_status_1_code             :=4 ;
        when g_rec_in.product_status_1_code = 'N'
            then g_rec_out.product_status_1_code             :=14 ;
        when g_rec_in.product_status_1_code = 'O'
            then g_rec_out.product_status_1_code             :=15;
        when g_rec_in.product_status_1_code = 'U'
            then g_rec_out.product_status_1_code             :=21 ;
        when g_rec_in.product_status_1_code = 'X'
            then g_rec_out.product_status_1_code             :=24;
        when g_rec_in.product_status_1_code = 'Z'
            then g_rec_out.product_status_1_code             :=26;
        when g_rec_in.product_status_1_code is null
            then g_rec_out.product_status_1_code             :=0;
        else g_rec_out.product_status_1_code             :=0 ;
  end case;

   g_rec_out.last_updated_date               := g_date;

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_zone_group_zone_no          := g_rec_in.sk1_zone_group_zone_no;

   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.NUM_EXTRA_LEADTIME_DAYS         := g_rec_in.NUM_EXTRA_LEAD_TIME_DAYS;            -- BK04Jun16   
   g_rec_out.FROM_LOC_NO                     := g_rec_in.FROM_LOC_NO;                         -- BK22Aug16
   
   g_rec_out.ship_hi                         := g_rec_in.ship_hi;   --ADDED OCTOBER 2014
   g_rec_out.ship_ti                         := g_rec_in.ship_ti;   --ADDED OCTOBER 2014

   begin
         select nvl(vat_rate_perc,0) vat_rate_perc
         into   g_vat_rate_perc
         from   dim_item
         where  item_no             = g_rec_out.sk1_item_no   ;
         exception
         when no_data_found then g_vat_rate_perc   := 15;
   end;

   --g_rec_out.reg_rsp_excl_vat          := g_rec_in.case_selling_incl_vat * 100 / (100 + g_vat_rate_perc);   --100 * (g_rec_in.case_selling_incl_vat / 114

   g_rec_out.reg_rsp_excl_vat          := g_rec_in.reg_rsp/(100 + g_vat_rate_perc) * 100;   --100 * (g_rec_in.case_selling_incl_vat / 114
--   g_rec_out.reg_rsp_excl_vat          := g_rec_in.reg_rsp_excl_vat;    --AJ-chgxxx
   g_rec_out.cost_price                := g_rec_in.cost_price;          --AJ-chgxxx PUT BACK NOV 2018
   --l_text := 'Item = ' || g_rec_in.item_no || ' zone = ' || g_rec_in.zone_no || ' RSP = ' || g_rec_in.reg_rsp || ' : reg_rsp_exl_vat = ' || g_rec_out.reg_rsp_excl_vat;
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_zone_item_om values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_zone_group_zone_no;
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
       update rtl_zone_item_om
       set    --base_retail_ind               = a_tbl_update(i).base_retail_ind,
              reg_rsp                       = a_tbl_update(i).reg_rsp,
              --selling_uom_code              = a_tbl_update(i).selling_uom_code,
              --market_basket_code            = a_tbl_update(i).market_basket_code,
              --item_zone_link_code           = a_tbl_update(i).item_zone_link_code,
              --multi_selling_uom_code        = a_tbl_update(i).multi_selling_uom_code,
              --selling_unit_rsp              = a_tbl_update(i).selling_unit_rsp,
              --multi_unit_rsp                = a_tbl_update(i).multi_unit_rsp,
              --multi_qty                     = a_tbl_update(i).multi_qty,
              case_mass                     = a_tbl_update(i).case_mass,                    --147
              case_selling_excl_vat         = a_tbl_update(i).case_selling_excl_vat,        --147
              case_selling                  = a_tbl_update(i).case_selling,        --147
              reg_rsp_excl_vat              = a_tbl_update(i).reg_rsp_excl_vat,           --147
              case_cost                     = a_tbl_update(i).case_cost,              --147       ** NOT SURE ABOUT THIS ONE !!
              num_units_per_tray            = a_tbl_update(i).num_units_per_tray,           --147
              product_status_code           = a_tbl_update(i).product_status_code,          --148
              product_status_1_code         = a_tbl_update(i).product_status_1_code,        --148
              mu_pack_height_code           = a_tbl_update(i).mu_pack_height_code,          --153
              num_shelf_life_days           = a_tbl_update(i).num_shelf_life_days,          --141
              tray_size_code                = a_tbl_update(i).tray_size_code,               --153
              mu_extsn_sleeve_code          = a_tbl_update(i).mu_extsn_sleeve_code,         --153
              min_order_qty                 = a_tbl_update(i).min_order_qty,                --153
              ship_hi                       = a_tbl_update(i).ship_hi,                      --153
              ship_ti                       = a_tbl_update(i).ship_ti,                      --153
              supplier_no                   = a_tbl_update(i).supplier_no,                  --148
              NUM_EXTRA_LEADTIME_DAYS       = a_tbl_update(i).NUM_EXTRA_LEADTIME_DAYS,                            -- BK04Jun16
              FROM_LOC_NO                   = a_tbl_update(i).FROM_LOC_NO,                                        -- BK22aug16
              last_updated_date             = a_tbl_update(i).last_updated_date,
              
              case_cost_price_local           = a_tbl_update(i).case_cost_price_local,
              case_cost_price_opr             = a_tbl_update(i).case_cost_price_opr,
              case_selling_excl_vat_local     = a_tbl_update(i).case_selling_excl_vat_local,
              case_selling_excl_vat_opr       = a_tbl_update(i).case_selling_excl_vat_opr,
              case_selling_incl_vat_local     = a_tbl_update(i).case_selling_incl_vat_local,
              case_selling_incl_vat_opr       = a_tbl_update(i).case_selling_incl_vat_opr,
              cost_price_local                = a_tbl_update(i).cost_price_local,
              cost_price_opr                  = a_tbl_update(i).cost_price_opr,
              reg_rsp_excl_vat_local          = a_tbl_update(i).reg_rsp_excl_vat_local,
              reg_rsp_excl_vat_opr            = a_tbl_update(i).reg_rsp_excl_vat_opr,
              reg_rsp_incl_vat_local          = a_tbl_update(i).reg_rsp_incl_vat_local,
              reg_rsp_incl_vat_opr            = a_tbl_update(i).reg_rsp_incl_vat_opr,
              reg_rsp_local                   = a_tbl_update(i).reg_rsp_local,
              reg_rsp_opr                     = a_tbl_update(i).reg_rsp_opr,
              selling_unit_rsp_local          = a_tbl_update(i).selling_unit_rsp_local,
              selling_unit_rsp_opr            = a_tbl_update(i).selling_unit_rsp_opr,
              multi_unit_rsp_local            = a_tbl_update(i).multi_unit_rsp_local,
              multi_unit_rsp_opr              = a_tbl_update(i).multi_unit_rsp_opr,

              
              cost_price                    = a_tbl_update(i).cost_price                                          -- AJ-chgxxx  PUT BACK NOV 2018
        where sk1_item_no                   = a_tbl_update(i).sk1_item_no  and
              sk1_zone_group_zone_no        = a_tbl_update(i).sk1_zone_group_zone_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no;
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
   into   g_count
   from   rtl_zone_item_om
   where  sk1_item_no             = g_rec_out.sk1_item_no  and
          sk1_zone_group_zone_no  = g_rec_out.sk1_zone_group_zone_no;

   if g_count = 1 then
      g_found := TRUE;
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

    l_text := 'LOAD OF RTL_ZONE_ITEM_OM EX FOUNDATION STARTED AT '||
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
    open c_fnd_zone_item;
    fetch c_fnd_zone_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_zone_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_zone_item;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;



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
end wh_prf_corp_052u;
