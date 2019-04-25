--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_738U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_738U" (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Sean Le Roux
--  Purpose:     Create Store Order Fact table in the foundation layer
--               with input ex staging table from OM.
--  Tables:      Input  - stg_om_st_ord_cpy
--               Output - fnd_rtl_loc_item_dy_om_st_ord
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  03 March 2009 - defect 911 - Records written to stg_om_st_ord_hsp
--                               but error not reflected in SYS_DWH_LOG
--                               ** Item_no is the problem
--  16 April 2009 - defect 689 - Remove fields from STG_OM_ST_ORD (608)...
--                               and related HSP,ARC,CPY tables
--                               AND derive in PRF layer
--  21 march 2012 - defect 4605 - Composites Correction: Number of Units per tray for Composite Items
--  24 march 2012 - defect 4606 - Composites Correction: SOH Adjusted & BOH Adjusted Values for Composite Items
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_count              number        :=  0;
g_hospital_text      stg_om_st_ord_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_loc_item_dy_om_st_ord%rowtype;
g_rec_in             stg_om_st_ord_cpy%rowtype;
g_found              boolean;
G_Valid              Boolean;
G_Pack_Type_Ind      Number        :=   0;
G_Cnt                Number        :=   0;
G_CNT2               Number        :=   0;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_738U'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STORE ORDERS EX OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_om_st_ord_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_loc_item_dy_om_st_ord%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_loc_item_dy_om_st_ord%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_om_st_ord_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_om_st_ord_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_om_st_ord is
   --select *
   --from stg_om_st_ord_cpy
   --where sys_process_code = 'N' and
   --      location_no      between p_from_loc_no and p_to_loc_no;
      
-- Foods Renewal dept live check -> must only process depts that are NOT coming from FF.
   select a.* 
     from stg_om_st_ord_cpy a, dim_item b, fnd_jdaff_dept_rollout c
   where 
   --sys_process_code = 'N' 
   --  and 
   a.item_no = b.item_no
     and b.department_no = c.department_no
     and c.department_live_ind = 'N'
     and a.location_no between p_from_loc_no and p_to_loc_no;
     
--   order by sys_source_batch_id,sys_source_sequence_no;
-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
v_count              number        :=  0;
begin
   g_rec_in.weigh_ind                := nvl(g_rec_in.weigh_ind,0);

   g_hospital                        := 'N';
   g_rec_out.post_date               := g_rec_in.post_date;
   g_rec_out.location_no             := g_rec_in.location_no;
   g_rec_out.item_no                 := g_rec_in.item_no;
   g_rec_out.dept_type               := g_rec_in.dept_type;
   g_rec_out.direct_delivery_ind     := g_rec_in.direct_delivery_ind;
   g_rec_out.num_store_leadtime_days := g_rec_in.num_store_leadtime_days;
   g_rec_out.boh_1_qty               := g_rec_in.boh_1_qty;
   g_rec_out.boh_1_ind               := g_rec_in.boh_1_ind;
   g_rec_out.boh_2_qty               := g_rec_in.boh_2_qty;
   g_rec_out.boh_3_qty               := g_rec_in.boh_3_qty;
   g_rec_out.sdn_1_qty               := g_rec_in.sdn_1_qty;
   g_rec_out.sdn1_ind                := g_rec_in.sdn1_ind;
   g_rec_out.sdn2_qty                := g_rec_in.sdn2_qty;
   g_rec_out.sdn2_ind                := g_rec_in.sdn2_ind;
--   g_rec_out.short_qty               := g_rec_in.short_qty;
   g_rec_out.day1_estimate           := g_rec_in.day1_estimate;
   g_rec_out.day2_estimate           := g_rec_in.day2_estimate;
   g_rec_out.day3_estimate           := g_rec_in.day3_estimate;
   g_rec_out.safety_qty              := g_rec_in.safety_qty;
   g_rec_out.model_stock             := g_rec_in.model_stock;
   g_rec_out.store_order1            := g_rec_in.store_order1;
   g_rec_out.store_order2            := g_rec_in.store_order2;
   g_rec_out.store_order3            := g_rec_in.store_order3;
   g_rec_out.delivery_pattern        := g_rec_in.delivery_pattern;
   g_rec_out.num_units_per_tray      := g_rec_in.num_units_per_tray;
   g_rec_out.weekly_estimate1        := g_rec_in.weekly_estimate1;
   g_rec_out.weekly_estimate2        := g_rec_in.weekly_estimate2;
   g_rec_out.shelf_life              := g_rec_in.shelf_life;
   g_rec_out.trading_date            := g_rec_in.trading_date;
  -- g_rec_out.sales_value             := g_rec_in.sales_value;
  -- g_rec_out.sales_qty               := g_rec_in.sales_qty;
  -- g_rec_out.waste_value             := g_rec_in.waste_value;
  -- g_rec_out.waste_qty               := g_rec_in.waste_qty;
   g_rec_out.prod_status_1           := g_rec_in.prod_status_1;
   g_rec_out.prod_status_2           := g_rec_in.prod_status_2;
   g_rec_out.day4_estimate           := g_rec_in.day4_estimate;
   g_rec_out.day5_estimate           := g_rec_in.day5_estimate;
   g_rec_out.day6_estimate           := g_rec_in.day6_estimate;
   g_rec_out.day7_estimate           := g_rec_in.day7_estimate;
   g_rec_out.day1_est_val2           := g_rec_in.day1_est_val2;
   g_rec_out.day2_est_val2           := g_rec_in.day2_est_val2;
   g_rec_out.day3_est_val2           := g_rec_in.day3_est_val2;
   g_rec_out.day4_est_val2           := g_rec_in.day4_est_val2;
   g_rec_out.day5_est_val2           := g_rec_in.day5_est_val2;
   g_rec_out.day6_est_val2           := g_rec_in.day6_est_val2;
   g_rec_out.day7_est_val2           := g_rec_in.day7_est_val2;
   g_rec_out.day1_est_unit2          := g_rec_in.day1_est_unit2;
   g_rec_out.day2_est_unit2          := g_rec_in.day2_est_unit2;
   g_rec_out.day3_est_unit2          := g_rec_in.day3_est_unit2;
   g_rec_out.day4_est_unit2          := g_rec_in.day4_est_unit2;
   g_rec_out.day5_est_unit2          := g_rec_in.day5_est_unit2;
   g_rec_out.day6_est_unit2          := g_rec_in.day6_est_unit2;
   g_rec_out.day7_est_unit2          := g_rec_in.day7_est_unit2;
   g_rec_out.num_units_per_tray2     := g_rec_in.num_units_per_tray2;
   g_rec_out.store_model_stock       := g_rec_in.store_model_stock;
   g_rec_out.day1_deliv_pat1         := g_rec_in.day1_deliv_pat1;
   g_rec_out.day2_deliv_pat1         := g_rec_in.day2_deliv_pat1;
   g_rec_out.day3_deliv_pat1         := g_rec_in.day3_deliv_pat1;
   g_rec_out.day4_deliv_pat1         := g_rec_in.day4_deliv_pat1;
   g_rec_out.day5_deliv_pat1         := g_rec_in.day5_deliv_pat1;
   g_rec_out.day6_deliv_pat1         := g_rec_in.day6_deliv_pat1;
   g_rec_out.day7_deliv_pat1         := g_rec_in.day7_deliv_pat1;
   g_rec_out.source_data_status_code := g_rec_in.source_data_status_code;
   g_rec_out.weigh_ind               := g_rec_in.weigh_ind;
   g_rec_out.last_updated_date       := g_date;


--   if not  dwh_valid.fnd_calendar(g_rec_out.post_date) then
   if  g_rec_out.post_date < (g_date - 730) or  g_rec_out.post_date > (g_date + 7) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_date_not_found;
     l_text          := dwh_constants.vc_date_not_found||g_rec_out.post_date ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;


   if not  dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.location_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

--   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_invalid_source_code;
--   end if;

   if g_rec_in.weigh_ind = 1 then
      g_rec_out.boh_1_qty               := g_rec_out.boh_1_qty / 1000;
      g_rec_out.boh_2_qty               := g_rec_out.boh_2_qty / 1000;
      g_rec_out.boh_3_qty               := g_rec_out.boh_3_qty / 1000;
      g_rec_out.sdn_1_qty               := g_rec_out.sdn_1_qty / 1000;
      g_rec_out.sdn2_qty                := g_rec_out.sdn2_qty / 1000;
--     g_rec_out.short_qty               := g_rec_out.short_qty / 1000;
      g_rec_out.day1_estimate           := g_rec_out.day1_estimate / 1000;
      g_rec_out.day2_estimate           := g_rec_out.day2_estimate / 1000;
      g_rec_out.day3_estimate           := g_rec_out.day3_estimate / 1000;
      g_rec_out.safety_qty              := g_rec_out.safety_qty / 1000;
--      g_rec_out.model_stock             := g_rec_out.model_stock / 1000;
      g_rec_out.store_order1            := g_rec_out.store_order1 / 1000;
      g_rec_out.store_order2            := g_rec_out.store_order2 / 1000;
      g_rec_out.store_order3            := g_rec_out.store_order3 / 1000;
      g_rec_out.weekly_estimate1        := g_rec_out.weekly_estimate1 / 1000;
      g_rec_out.weekly_estimate2        := g_rec_out.weekly_estimate2 / 1000;
  --    g_rec_out.sales_qty               := g_rec_out.sales_qty / 1000;
  --    g_rec_out.waste_qty               := g_rec_out.waste_qty / 1000;

      g_rec_out.day4_estimate           := g_rec_out.day4_estimate / 1000;
      g_rec_out.day5_estimate           := g_rec_out.day5_estimate / 1000;
      g_rec_out.day6_estimate           := g_rec_out.day6_estimate / 1000;
      g_rec_out.day7_estimate           := g_rec_out.day7_estimate / 1000;
      g_rec_out.day1_est_unit2          := g_rec_out.day1_est_unit2 / 1000;
      g_rec_out.day2_est_unit2          := g_rec_out.day2_est_unit2 / 1000;
      g_rec_out.day3_est_unit2          := g_rec_out.day3_est_unit2 / 1000;
      g_rec_out.day4_est_unit2          := g_rec_out.day4_est_unit2 / 1000;
      g_rec_out.day5_est_unit2          := g_rec_out.day5_est_unit2 / 1000;
      g_rec_out.day6_est_unit2          := g_rec_out.day6_est_unit2 / 1000;
      g_rec_out.day7_est_unit2          := g_rec_out.day7_est_unit2 / 1000;
   end if;

--------------------
--      qc4605  and 4606   start
--------------------
--(Case item_no from dim_item.item_no
--When (Pack_Item_Ind = 0 And Pack_Item_Simple_Ind = 0) Then '1 - single item'
--When (Pack_Item_Ind = 1 And Pack_Item_Simple_Ind = 1) Then '2 - simple pack'
--When (Pack_Item_Ind = 1 And Pack_Item_Simple_Ind = 0) Then '3 - composite pack'
--Else '0 - not listed' End) New_Item_Type_Ind Into G_Pack_Type
IF G_Rec_Out.Num_Units_Per_Tray IS NOT NULL THEN
  G_Pack_Type_ind               := 0;
  BEGIN
    SELECT (
      CASE
        WHEN (Pack_Item_Ind      = 0
        AND Pack_Item_Simple_Ind = 0)
        THEN 1
        WHEN (Pack_Item_Ind      = 1
        AND Pack_Item_Simple_Ind = 1)
        THEN 2
        WHEN (Pack_Item_Ind      = 1
        AND Pack_Item_Simple_Ind = 0)
        THEN 3
        ELSE 0
      END) New_Item_Type_Ind
    INTO G_Pack_Type_ind
    FROM Fnd_Item A
    WHERE A.Item_No = G_Rec_Out.Item_No;
  EXCEPTION
  WHEN no_data_found THEN
    NULL;
  WHEN OTHERS THEN
    l_message := 'fnd_item lookup failure - defaulting to New_Item_Type_Ind = 0'||SQLCODE||' '||sqlerrm;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
    Raise;
  END;
  IF G_Pack_Type_Ind              = 3 THEN
    G_Rec_Out.Num_Units_Per_Tray := 1;
    g_rec_out. BOH_2_qty         := 0;  -- qc4606--
           G_Cnt := G_Cnt + 1;
  End If;
--
--- Num_Units_per_tray2
--
  IF G_Rec_Out.Num_Units_Per_Tray2 IS NOT NULL THEN
   If G_Pack_Type_Ind              = 3 Then
    G_Rec_Out.Num_Units_Per_Tray2 := 1;
           g_cnt2 := g_cnt2 + 1;
  End If;
END IF;
End If;

--------------------
--      qc4605  and 4606   end
--------------------

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

   insert into stg_om_st_ord_hsp
        values g_rec_in;

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
       insert into fnd_rtl_loc_item_dy_om_st_ord values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).post_date||
                       ' '||a_tbl_insert(g_error_index).location_no||
                       ' '||a_tbl_insert(g_error_index).item_no;
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
       update fnd_rtl_loc_item_dy_om_st_ord
       set    dept_type                       = a_tbl_update(i).dept_type,
              direct_delivery_ind             = a_tbl_update(i).direct_delivery_ind,
              num_store_leadtime_days         = a_tbl_update(i).num_store_leadtime_days,
              boh_1_qty                       = a_tbl_update(i).boh_1_qty,
              boh_1_ind                       = a_tbl_update(i).boh_1_ind,
              boh_2_qty                       = a_tbl_update(i).boh_2_qty,
              boh_3_qty                       = a_tbl_update(i).boh_3_qty,
              sdn_1_qty                       = a_tbl_update(i).sdn_1_qty,
              sdn1_ind                        = a_tbl_update(i).sdn1_ind,
              sdn2_qty                        = a_tbl_update(i).sdn2_qty,
              sdn2_ind                        = a_tbl_update(i).sdn2_ind,
 --             short_qty                       = a_tbl_update(i).short_qty,
              day1_estimate                   = a_tbl_update(i).day1_estimate,
              day2_estimate                   = a_tbl_update(i).day2_estimate,
              day3_estimate                   = a_tbl_update(i).day3_estimate,
              safety_qty                      = a_tbl_update(i).safety_qty,
              model_stock                     = a_tbl_update(i).model_stock,
              store_order1                    = a_tbl_update(i).store_order1,
              store_order2                    = a_tbl_update(i).store_order2,
              store_order3                    = a_tbl_update(i).store_order3,
              delivery_pattern                = a_tbl_update(i).delivery_pattern,
              num_units_per_tray              = a_tbl_update(i).num_units_per_tray,
              weekly_estimate1                = a_tbl_update(i).weekly_estimate1,
              weekly_estimate2                = a_tbl_update(i).weekly_estimate2,
              shelf_life                      = a_tbl_update(i).shelf_life,
              trading_date                    = a_tbl_update(i).trading_date,
 --             sales_value                     = a_tbl_update(i).sales_value,
 --             sales_qty                       = a_tbl_update(i).sales_qty,
 --             waste_value                     = a_tbl_update(i).waste_value,
 --             waste_qty                       = a_tbl_update(i).waste_qty,
              prod_status_1                   = a_tbl_update(i).prod_status_1,
              prod_status_2                   = a_tbl_update(i).prod_status_2,

              day4_estimate                   = a_tbl_update(i).day4_estimate,
              day5_estimate                   = a_tbl_update(i).day5_estimate,
              day6_estimate                   = a_tbl_update(i).day6_estimate,
              day7_estimate                   = a_tbl_update(i).day7_estimate,
              day1_est_val2                   = a_tbl_update(i).day1_est_val2,
              day2_est_val2                   = a_tbl_update(i).day2_est_val2,
              day3_est_val2                   = a_tbl_update(i).day3_est_val2,
              day4_est_val2                   = a_tbl_update(i).day4_est_val2,
              day5_est_val2                   = a_tbl_update(i).day5_est_val2,
              day6_est_val2                   = a_tbl_update(i).day6_est_val2,
              day7_est_val2                   = a_tbl_update(i).day7_est_val2,
              day1_est_unit2                  = a_tbl_update(i).day1_est_unit2,
              day2_est_unit2                  = a_tbl_update(i).day2_est_unit2,
              day3_est_unit2                  = a_tbl_update(i).day3_est_unit2,
              day4_est_unit2                  = a_tbl_update(i).day4_est_unit2,
              day5_est_unit2                  = a_tbl_update(i).day5_est_unit2,
              day6_est_unit2                  = a_tbl_update(i).day6_est_unit2,
              day7_est_unit2                  = a_tbl_update(i).day7_est_unit2,
              num_units_per_tray2             = a_tbl_update(i).num_units_per_tray2,
              store_model_stock               = a_tbl_update(i).store_model_stock,
              day1_deliv_pat1                 = a_tbl_update(i).day1_deliv_pat1,
              day2_deliv_pat1                 = a_tbl_update(i).day2_deliv_pat1,
              day3_deliv_pat1                 = a_tbl_update(i).day3_deliv_pat1,
              day4_deliv_pat1                 = a_tbl_update(i).day4_deliv_pat1,
              day5_deliv_pat1                 = a_tbl_update(i).day5_deliv_pat1,
              day6_deliv_pat1                 = a_tbl_update(i).day6_deliv_pat1,
              day7_deliv_pat1                 = a_tbl_update(i).day7_deliv_pat1,
              source_data_status_code = a_tbl_update(i).source_data_status_code,
              weigh_ind                       = a_tbl_update(i).weigh_ind,
              last_updated_date       = a_tbl_update(i).last_updated_date
       where  post_date             = a_tbl_update(i).post_date
         and  location_no             = a_tbl_update(i).location_no
         and  item_no                 = a_tbl_update(i).item_no;


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
                       ' '||a_tbl_update(g_error_index).post_date||
                       ' '||a_tbl_update(g_error_index).location_no||
                       ' '||a_tbl_update(g_error_index).item_no;
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
       update stg_om_st_ord_cpy
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
  select count(1)
   into   g_count
   from   fnd_rtl_loc_item_dy_om_st_ord
   where  location_no      = g_rec_out.location_no
     and  item_no          = g_rec_out.item_no
     and  post_date      = g_rec_out.post_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).post_date = g_rec_out.post_date and
            a_tbl_insert(i).location_no = g_rec_out.location_no and
            a_tbl_insert(i).item_no = g_rec_out.item_no then
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
--      local_bulk_staging_update;

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

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_rtl_loc_item_dy_om_st_ord EX OM STARTED AT '||
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
    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_om_st_ord;
    fetch c_stg_om_st_ord bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_om_st_ord bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_om_st_ord;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
--    local_bulk_staging_update;
l_text := 'records altered ='||G_cnt ;
 Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
 l_text := 'records altered 2 ='||G_cnt2 ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
End Wh_Fnd_Corp_738u;
