--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_104U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_104U" 
                                                                           (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Create zone_item dimention table in the foundation layer
--               with input ex staging table from OM.
--  Tables:      Input  - stg_om_zone_item_cpy
--               Output - fnd_zone_item_om
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  21 march 2012 - defect 4605 - Composites Correction: Number of Units per tray for Composite Items
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
g_vat_rate_perc      dim_item.vat_rate_perc%type;
g_hospital_text      stg_om_zone_item_hsp.sys_process_msg%type;
g_rec_out            fnd_zone_item_om%rowtype;
g_rec_in             stg_om_zone_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_count              integer       :=  0;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
G_Date               Date          := Trunc(Sysdate);
G_Pack_Type_Ind      Number;
g_cnt                number;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_104U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE_ITEM MASTERDATA EX OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_om_zone_item_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_zone_item_om%rowtype index by binary_integer;
type tbl_array_u is table of fnd_zone_item_om%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_om_zone_item_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_om_zone_item_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_om_zone_item is
   --select *
   --from stg_om_zone_item_cpy
   --where sys_process_code = 'N'
   --order by sys_source_batch_id,sys_source_sequence_no;
   
  select a.*
   from stg_om_zone_item_cpy a, dim_item b, fnd_jdaff_dept_rollout c
   where a.sys_process_code = 'N'
     and a.item_no = b.item_no
     and b.department_no = c.department_no
     and c.department_live_ind = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                               := 'N';

   g_rec_out.zone_group_no                   := g_rec_in.zone_group_no;
   g_rec_out.zone_no                         := g_rec_in.zone_no;
   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.direct_delivery_ind             := g_rec_in.direct_delivery_ind;
   g_rec_out.from_loc_no                     := g_rec_in.from_loc_no;
   g_rec_out.case_mass                       := g_rec_in.case_mass;
   g_rec_out.tray_size_code                  := g_rec_in.tray_size_code;
   g_rec_out.num_units_per_tray              := g_rec_in.num_units_per_tray;
--   g_rec_out.case_selling_excl_vat           := g_rec_in.case_selling_excl_vat;
   g_rec_out.case_cost                       := g_rec_in.case_cost;
   g_rec_out.product_status_code             := g_rec_in.product_status_code;
--   g_rec_out.reg_rsp_excl_vat                := g_rec_in.reg_rsp_excl_vat;
--   g_rec_out.cost_price                      := g_rec_in.cost_price;
   g_rec_out.num_shelf_life_days             := g_rec_in.num_shelf_life_days;
   g_rec_out.num_supplier_leadtime_days      := g_rec_in.num_supplier_leadtime_days;
   g_rec_out.banker_ind                      := g_rec_in.banker_ind;
   g_rec_out.early_delivery_ind              := g_rec_in.early_delivery_ind;
   g_rec_out.mu_pack_height_code             := g_rec_in.mu_pack_height_code;
   g_rec_out.mu_extsn_sleeve_code            := g_rec_in.mu_extsn_sleeve_code;
   g_rec_out.supplier_no                     := g_rec_in.supplier_no;
   g_rec_out.num_extra_leadtime_days         := g_rec_in.num_extra_leadtime_days;
   g_rec_out.reg_delivery_pattern_code       := g_rec_in.reg_delivery_pattern_code;
   g_rec_out.reg_shelf_life_day_1            := g_rec_in.reg_shelf_life_day_1;
   g_rec_out.reg_shelf_life_day_2            := g_rec_in.reg_shelf_life_day_2;
   g_rec_out.reg_shelf_life_day_3            := g_rec_in.reg_shelf_life_day_3;
   g_rec_out.reg_shelf_life_day_4            := g_rec_in.reg_shelf_life_day_4;
   g_rec_out.reg_shelf_life_day_5            := g_rec_in.reg_shelf_life_day_5;
   g_rec_out.reg_shelf_life_day_6            := g_rec_in.reg_shelf_life_day_6;
   g_rec_out.reg_shelf_life_day_7            := g_rec_in.reg_shelf_life_day_7;
   g_rec_out.product_status_1_code           := g_rec_in.product_status_1_code;
   g_rec_out.num_mu_per_tray                 := g_rec_in.num_mu_per_tray;
   g_rec_out.reg_rsp                         := g_rec_in.reg_rsp;
   g_rec_out.case_selling                    := g_rec_in.case_selling;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.weigh_ind                       := g_rec_in.weigh_ind;
   g_rec_out.last_updated_date               := g_date;


--   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_invalid_source_code;
--   end if;

   if not dwh_valid.indicator_field(g_rec_out.direct_delivery_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.weigh_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.banker_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.early_delivery_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not  dwh_valid.fnd_zone(g_rec_out.zone_no,g_rec_out.zone_group_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_zone_not_found;
     l_text          := dwh_constants.vc_zone_not_found||g_rec_out.zone_no||' '||g_rec_out.zone_group_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not  dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not  dwh_valid.fnd_location(g_rec_out.from_loc_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_out.from_loc_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if not  dwh_valid.fnd_supplier(g_rec_out.supplier_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_supplier_not_found;
     l_text          := dwh_constants.vc_supplier_not_found||g_rec_out.supplier_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;

   if g_rec_out.weigh_ind = 1 then
      g_rec_out.num_units_per_tray   :=   g_rec_out.num_units_per_tray/ 1000;
   end if;

--------------------
--      qc4605 - start
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
  If G_Pack_Type_Ind              = 3 Then
    G_Rec_Out.Num_Units_Per_Tray := 1;
       g_cnt := g_cnt + 1;
  End If;

END IF;
--------------------
--      qc4605   end
--------------------

   begin
         select nvl(vat_rate_perc,0) vat_rate_perc
         into   g_vat_rate_perc
         from   dim_item
         where  item_no             = g_rec_out.item_no   ;
         exception
         when no_data_found then
           g_vat_rate_perc   := 14;
   end;
   g_rec_out.reg_rsp_excl_vat      := g_rec_in.reg_rsp_excl_vat      * 100 / (100 + g_vat_rate_perc) ;
   G_Rec_Out.Case_Selling_Excl_Vat := G_Rec_In.Case_Selling_Excl_Vat * 100 / (100 + G_Vat_Rate_Perc) ;
--      qc4605   START
   G_Rec_Out.Cost_Price            := G_Rec_in.Case_Cost / G_Rec_out.Num_Units_Per_Tray;
--   g_rec_out.cost_price            := g_rec_in.case_cost / g_rec_in.num_units_per_tray;
--      qc4605   END

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

   insert into stg_om_zone_item_hsp values g_rec_in;
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
       insert into fnd_zone_item_om values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).zone_no||
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
       update fnd_zone_item_om
       set    direct_delivery_ind             = a_tbl_update(i).direct_delivery_ind,
              from_loc_no                     = a_tbl_update(i).from_loc_no,
              case_mass                       = a_tbl_update(i).case_mass,
              tray_size_code                  = a_tbl_update(i).tray_size_code,
              num_units_per_tray              = a_tbl_update(i).num_units_per_tray,
              case_selling_excl_vat           = a_tbl_update(i).case_selling_excl_vat,
              case_cost                       = a_tbl_update(i).case_cost,
              product_status_code             = a_tbl_update(i).product_status_code,
              reg_rsp_excl_vat                = a_tbl_update(i).reg_rsp_excl_vat,
              cost_price                      = a_tbl_update(i).cost_price,
              num_shelf_life_days             = a_tbl_update(i).num_shelf_life_days,
              num_supplier_leadtime_days      = a_tbl_update(i).num_supplier_leadtime_days,
              banker_ind                      = a_tbl_update(i).banker_ind,
              early_delivery_ind              = a_tbl_update(i).early_delivery_ind,
              mu_pack_height_code             = a_tbl_update(i).mu_pack_height_code,
              mu_extsn_sleeve_code            = a_tbl_update(i).mu_extsn_sleeve_code,
              supplier_no                     = a_tbl_update(i).supplier_no,
              num_extra_leadtime_days         = a_tbl_update(i).num_extra_leadtime_days,
              reg_delivery_pattern_code       = a_tbl_update(i).reg_delivery_pattern_code,
              reg_shelf_life_day_1            = a_tbl_update(i).reg_shelf_life_day_1,
              reg_shelf_life_day_2            = a_tbl_update(i).reg_shelf_life_day_2,
              reg_shelf_life_day_3            = a_tbl_update(i).reg_shelf_life_day_3,
              reg_shelf_life_day_4            = a_tbl_update(i).reg_shelf_life_day_4,
              reg_shelf_life_day_5            = a_tbl_update(i).reg_shelf_life_day_5,
              reg_shelf_life_day_6            = a_tbl_update(i).reg_shelf_life_day_6,
              reg_shelf_life_day_7            = a_tbl_update(i).reg_shelf_life_day_7,
              product_status_1_code           = a_tbl_update(i).product_status_1_code,
              num_mu_per_tray                 = a_tbl_update(i).num_mu_per_tray,
              reg_rsp                         = a_tbl_update(i).reg_rsp,
              case_selling                    = a_tbl_update(i).case_selling,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              weigh_ind                       = a_tbl_update(i).weigh_ind,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  zone_no                         = a_tbl_update(i).zone_no and
              zone_group_no                   = a_tbl_update(i).zone_group_no and
              item_no                         = a_tbl_update(i).item_no;

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
                       ' '||a_tbl_update(g_error_index).zone_no||
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
       update stg_om_zone_item_cpy
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
   from   fnd_zone_item_om
   where  zone_no       = g_rec_out.zone_no and
          zone_group_no = g_rec_out.zone_group_no and
          item_no       = g_rec_out.item_no;

  if g_count = 1 then
     g_found := TRUE;
  end if;


-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).zone_no       = g_rec_out.zone_no and
            a_tbl_insert(i).zone_group_no = g_rec_out.zone_group_no and
            a_tbl_insert(i).item_no       = g_rec_out.item_no then
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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_ZONE_ITEM_OM EX OM STARTED AT '||
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
    open c_stg_om_zone_item;
    fetch c_stg_om_zone_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_om_zone_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_om_zone_item;
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
End Wh_Fnd_Corp_104u;
