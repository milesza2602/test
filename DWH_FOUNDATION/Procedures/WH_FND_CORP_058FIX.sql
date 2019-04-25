--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_058FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_058FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Sean Le Roux
--  Purpose:     Insert/Update ITEM dimension table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      AIT Input - stg_rms_item
--               Input     - stg_rms_item_cpy
--               Output    - fnd_item
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  11 Feb 2009 - defect 843- Default selling_ind and buying_ind on FND_ITEM on insert
--  11 Mar 2009 - QC 1101 - Referential Integrity rules on DIFF_#_CODE (STG_RMS_ITEM_CPY) to change
--  qc 1582 - 18 May 2009 - Set RDF_FORECST_IND to 0 if it's null

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
g_recs_integ         integer       :=  0;
g_recs_rejected      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_item_hsp.sys_process_msg%type;
g_rec_out            fnd_item%rowtype;
g_rec_in             stg_rms_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_style_colour_no    fnd_item.style_colour_no%type;
g_fnd_style_colour_no fnd_item.style_colour_no%type;
g_fnd_style_no       fnd_item.style_no%type;
g_style_no           fnd_item.style_no%type;
g_subclass_no        fnd_item.subclass_no%type;

g_restructure_ind    dim_control.restructure_ind%type;
g_sclass_no          fnd_item.subclass_no%type;
g_class_no           fnd_item.class_no%type;
g_department_no      fnd_item.department_no%type;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_058U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_rms_item_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_item%rowtype index by binary_integer;
type tbl_array_u is table of fnd_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_item_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_item_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_rms_item is
   select *
   from stg_rms_item_cpy
   where sys_process_code = 'N'
   --and item_no not in (5050048273206,94800347045,94800347052,94800347021,94800347038)   --Reclass Feb 2014
   order by sys_source_batch_id,sys_source_sequence_no;
   
 --  select a.*
 -- from stg_rms_item_cpy a, dim_item b, fnd_jdaff_dept_rollout c
 --  where a.sys_process_code = 'N'
 --    and a.item_no = b.item_no
 --    and b.department_no = c.department_no
  --   and c.department_live_ind = 'N'
 --  order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

cursor c_fnd_item is
   select distinct style_colour_no,style_no,subclass_no,CLASS_NO,DEPARTMENT_NO
   from   fnd_item
   where  last_updated_date = g_date and
          item_level_no    >= tran_level_no;
    
cursor c_fnd_item_old is
   select item_no,style_colour_no,style_no,subclass_no,CLASS_NO,DEPARTMENT_NO
   from   fnd_item
   where
--          last_updated_date < g_date and
-- This line checks only previous data. leaving it out checks all data
          style_colour_no =  g_style_colour_no and
          (style_no       <> g_style_no or
           subclass_no    <> g_subclass_no OR 
            CLASS_NO      <> G_CLASS_NO OR
            DEPARTMENT_NO <> G_DEPARTMENT_NO);

cursor c_fnd_item_s is
   select distinct style_no,subclass_no,CLASS_NO,DEPARTMENT_NO
   from   fnd_item
   where  last_updated_date = g_date and
          item_level_no    >= tran_level_no;
     
cursor c_fnd_item_old_s is
   select item_no,style_no,subclass_no,CLASS_NO,DEPARTMENT_NO
   from   fnd_item
   where
           style_no       = g_style_no and
           (subclass_no   <> g_subclass_no OR 
            CLASS_NO      <> G_CLASS_NO OR
            DEPARTMENT_NO <> G_DEPARTMENT_NO);



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';

   g_rec_out.item_no                         := g_rec_in.item_no;
   g_rec_out.item_desc                       := g_rec_in.item_desc;
   g_rec_out.item_short_desc                 := g_rec_in.item_short_desc;
   g_rec_out.item_upper_desc                 := g_rec_in.item_upper_desc;
   g_rec_out.item_scndry_desc                := g_rec_in.item_scndry_desc;
   g_rec_out.item_status_code                := g_rec_in.item_status_code;
   g_rec_out.item_level_no                   := g_rec_in.item_level_no;
   g_rec_out.tran_level_no                   := g_rec_in.tran_level_no;
   g_rec_out.primary_ref_item_ind            := g_rec_in.primary_ref_item_ind;
   g_rec_out.item_parent_no                  := g_rec_in.item_parent_no;
   g_rec_out.item_grandparent_no             := g_rec_in.item_grandparent_no;
   g_rec_out.item_level1_no                  := g_rec_in.item_level1_no;
   g_rec_out.item_level2_no                  := g_rec_in.item_level2_no;
   g_rec_out.subclass_no                     := g_rec_in.subclass_no;
   g_rec_out.class_no                        := g_rec_in.class_no;
   g_rec_out.department_no                   := g_rec_in.department_no;
   g_rec_out.rpl_ind                         := g_rec_in.rpl_ind;
   g_rec_out.item_no_type                    := g_rec_in.item_no_type;
   g_rec_out.format_id                       := g_rec_in.format_id;
   g_rec_out.upc_prefix_no                   := g_rec_in.upc_prefix_no;
   g_rec_out.diff_1_code                     := g_rec_in.diff_1_code;
   g_rec_out.diff_2_code                     := g_rec_in.diff_2_code;
   g_rec_out.diff_3_code                     := g_rec_in.diff_3_code;
   g_rec_out.diff_4_code                     := g_rec_in.diff_4_code;
   g_rec_out.item_aggr_ind                   := g_rec_in.item_aggr_ind;
   g_rec_out.diff_1_aggr_ind                 := g_rec_in.diff_1_aggr_ind;
   g_rec_out.diff_2_aggr_ind                 := g_rec_in.diff_2_aggr_ind;
   g_rec_out.diff_3_aggr_ind                 := g_rec_in.diff_3_aggr_ind;
   g_rec_out.diff_4_aggr_ind                 := g_rec_in.diff_4_aggr_ind;
   g_rec_out.retail_zone_group_no            := g_rec_in.retail_zone_group_no;
   g_rec_out.cost_zone_group_no              := g_rec_in.cost_zone_group_no;
   g_rec_out.standard_uom_code               := g_rec_in.standard_uom_code;
   g_rec_out.standard_uom_desc               := g_rec_in.standard_uom_desc;
   g_rec_out.standard_uom_class_code         := g_rec_in.standard_uom_class_code;
   g_rec_out.uom_conv_factor                 := g_rec_in.uom_conv_factor;
   g_rec_out.package_size                    := g_rec_in.package_size;
   g_rec_out.package_uom_code                := g_rec_in.package_uom_code;
   g_rec_out.package_uom_desc                := g_rec_in.package_uom_desc;
   g_rec_out.package_uom_class_code          := g_rec_in.package_uom_class_code;
   g_rec_out.merchandise_item_ind            := g_rec_in.merchandise_item_ind;
   g_rec_out.store_ord_mult_unit_type_code   := g_rec_in.store_ord_mult_unit_type_code;
   g_rec_out.ext_sys_forecast_ind            := g_rec_in.ext_sys_forecast_ind;
   g_rec_out.primary_currency_original_rsp   := g_rec_in.primary_currency_original_rsp;
   g_rec_out.mfg_recommended_rsp             := g_rec_in.mfg_recommended_rsp;
   g_rec_out.retail_label_type               := g_rec_in.retail_label_type;
   g_rec_out.retail_label_value              := g_rec_in.retail_label_value;
   g_rec_out.handling_temp_code              := g_rec_in.handling_temp_code;
   g_rec_out.handling_sensitivity_code       := g_rec_in.handling_sensitivity_code;
   g_rec_out.random_mass_ind                 := g_rec_in.random_mass_ind;
   g_rec_out.first_received_date             := g_rec_in.first_received_date;
   g_rec_out.last_received_date              := g_rec_in.last_received_date;
   g_rec_out.most_recent_received_qty        := g_rec_in.most_recent_received_qty;
   g_rec_out.waste_type                      := g_rec_in.waste_type;
   g_rec_out.avg_waste_perc                  := g_rec_in.avg_waste_perc;
   g_rec_out.default_waste_perc              := g_rec_in.default_waste_perc;
   g_rec_out.constant_dimension_ind          := g_rec_in.constant_dimension_ind;
   g_rec_out.pack_item_ind                   := g_rec_in.pack_item_ind;
   g_rec_out.pack_item_simple_ind            := g_rec_in.pack_item_simple_ind;
   g_rec_out.pack_item_inner_pack_ind        := g_rec_in.pack_item_inner_pack_ind;
   g_rec_out.pack_item_sellable_unit_ind     := g_rec_in.pack_item_sellable_unit_ind;
   g_rec_out.pack_item_orderable_ind         := g_rec_in.pack_item_orderable_ind;
   g_rec_out.pack_item_type                  := g_rec_in.pack_item_type;
   g_rec_out.pack_item_receivable_type       := g_rec_in.pack_item_receivable_type;
   g_rec_out.item_comment                    := g_rec_in.item_comment;
   g_rec_out.item_service_level_type         := g_rec_in.item_service_level_type;
   g_rec_out.gift_wrap_ind                   := g_rec_in.gift_wrap_ind;
   g_rec_out.ship_alone_ind                  := g_rec_in.ship_alone_ind;
   g_rec_out.origin_item_ext_src_sys_name    := g_rec_in.origin_item_ext_src_sys_name;
   g_rec_out.banded_item_ind                 := g_rec_in.banded_item_ind;
   g_rec_out.static_mass                     := g_rec_in.static_mass;
   g_rec_out.ext_ref_id                      := g_rec_in.ext_ref_id;
   g_rec_out.create_date                     := g_rec_in.create_date;
   g_rec_out.size_id                         := g_rec_in.size_id;
   g_rec_out.color_id                        := g_rec_in.color_id;
   g_rec_out.style_colour_no                 := g_rec_in.style_colour_no;
   g_rec_out.style_no                        := g_rec_in.style_no;
--   g_rec_out.base_rsp                        := g_rec_in.base_rsp;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.primary_supplier_no             := g_rec_in.primary_supplier_no;
   --
   -- these indicators must be defaulted to 0 only when inserting and
   -- not when updating as these will be updated in wh_fnd_prf_060u
      g_rec_out.selling_ind                     := 0;
      g_rec_out.buying_ind                      := 0;
   --
   -- these indicators must be defaulted to 0 only when inserting and
   -- not when updating as these will be updated in wh_fnd_rdf_020u
   g_rec_out.rdf_forecst_ind                 := 0;
   
   --Foods Renewal Fields
   g_rec_out.FD_PRODUCT_NO                      := g_rec_in.FD_PRODUCT_NO;
   g_rec_out.PRODUCT_CLASS                      := g_rec_in.PRODUCT_CLASS;
   g_rec_out.FD_DISCIPLINE_TYPE                 := g_rec_in.FD_DISCIPLINE_TYPE;
   g_rec_out.HANDLING_METHOD_CODE               := g_rec_in.HANDLING_METHOD_CODE;
   g_rec_out.HANDLING_METHOD_NAME               := g_rec_in.HANDLING_METHOD_NAME;
   g_rec_out.DISPLAY_METHOD_CODE                := g_rec_in.DISPLAY_METHOD_CODE;
   g_rec_out.DISPLAY_METHOD_NAME                := g_rec_in.DISPLAY_METHOD_NAME;
   g_rec_out.SEGREGATION_IND                    := g_rec_in.SEGREGATION_IND;
   g_rec_out.LOOSE_PROD_IND                     := g_rec_in.LOOSE_PROD_IND;
   g_rec_out.VAR_WEIGHT_IND                     := g_rec_in.VAR_WEIGHT_IND;
   --
   g_rec_out.last_updated_date               := g_date;

   if g_rec_out.item_level_no < g_rec_out.tran_level_no then
      if g_rec_out.style_no is null then
         g_rec_out.style_no := 0;
      end if;
      if g_rec_out.style_colour_no is null then
         g_rec_out.style_colour_no := 0;
      end if;
   end if;
   if g_rec_out.primary_supplier_no is null then
      g_rec_out.primary_supplier_no := 0;
   else
      if not dwh_valid.fnd_supplier(g_rec_out.primary_supplier_no) then
        g_hospital      := 'Y';
        g_hospital_text := dwh_constants.vc_supplier_not_found;
        l_text          := dwh_constants.vc_supplier_not_found||' '||g_rec_out.primary_supplier_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        return;
      end if;
   end if;

-- Validating Foreign Keys of SUBCLASS, CLASS and DEPARTMENT

   if not dwh_valid.fnd_subclass(g_rec_out.subclass_no,g_rec_out.class_no,g_rec_out.department_no) then
      g_hospital      := 'Y';
      g_hospital_text := dwh_constants.vc_subclass_not_found;
      l_text          := dwh_constants.vc_subclass_not_found||' '||g_rec_out.subclass_no||' '||g_rec_out.class_no||' '||g_rec_out.department_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      return;
   end if;

   if g_restructure_ind = 0 then
      begin
        select subclass_no,class_no,department_no,style_no, style_colour_no
        into   g_sclass_no,g_class_no,g_department_no,g_fnd_style_no, g_fnd_style_colour_no
        from   fnd_item
        where  item_no = g_rec_out.item_no;

        exception
        when no_data_found then
          g_sclass_no            := g_rec_out.subclass_no;
          g_class_no             := g_rec_out.class_no;
          g_department_no        := g_rec_out.department_no;
          g_fnd_style_no         := g_rec_out.style_no;
          g_fnd_style_colour_no  := g_rec_out.style_colour_no;
      end;

      if g_sclass_no     <> g_rec_out.subclass_no or
         g_class_no      <> g_rec_out.class_no    or
         g_department_no <> g_rec_out.department_no then
         dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                  'fnd_item',g_rec_out.item_no,g_sclass_no,g_rec_out.subclass_no);
         g_hospital      := 'Y';
         g_hospital_text := 'Trying to illegally restructure hierarchy ';
         l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.item_no||' '||g_rec_out.subclass_no  ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      if g_fnd_style_no     <> g_rec_out.style_no  then
         dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                  'fnd_item',g_rec_out.item_no,g_fnd_style_no,g_rec_out.style_no);
         g_hospital      := 'Y';
         g_hospital_text := 'Trying to illegally restructure style ';
         l_text          := 'Trying to illegally restructure style '||g_rec_out.item_no||' '||g_rec_out.style_no  ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      if g_fnd_style_colour_no  <> g_rec_out.style_colour_no  then
         dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                  'fnd_item',g_rec_out.item_no,g_fnd_style_colour_no,g_rec_out.style_colour_no);
         g_hospital      := 'Y';
         g_hospital_text := 'Trying to illegally restructure style colour ';
         l_text          := 'Trying to illegally restructure style colour '||g_rec_out.item_no||' '||g_rec_out.style_colour_no  ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
   end if;


--   if not dwh_valid.fnd_class(g_rec_out.class_no,g_rec_out.department_no) then
--      g_hospital      := 'Y';
--      g_hospital_text := dwh_constants.vc_class_not_found;
--      l_text          := dwh_constants.vc_class_not_found||' '||g_rec_out.class_no||' '||g_rec_out.department_no;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      return;
--   end if;

--   if not dwh_valid.fnd_department(g_rec_out.department_no) then
--      g_hospital      := 'Y';
--      g_hospital_text := dwh_constants.vc_dept_not_found;
--      l_text          := dwh_constants.vc_dept_not_found||' '||g_rec_out.department_no;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      return;
--   end if;



   if g_rec_out.item_level_no >= g_rec_out.tran_level_no then
      if g_rec_out.style_no is null or
         g_rec_out.style_no = 0     then
         g_hospital      := 'Y';
         g_hospital_text := 'Style no must be a positive integer';
         return;
      end if;
      if g_rec_out.style_colour_no is null or
         g_rec_out.style_colour_no = 0     then
         g_hospital      := 'Y';
         g_hospital_text := 'Style_Colour no must be a positive integer';
         return;
      end if;
   end if;


   if g_rec_out.diff_1_code is not null then
 --     if g_rec_out.item_level_no < g_rec_out.tran_level_no then
         if not dwh_valid.fnd_diff_group(g_rec_out.diff_1_code) then
            if not dwh_valid.fnd_diff(g_rec_out.diff_1_code) then
            g_hospital      := 'Y';
            g_hospital_text := dwh_constants.vc_diff_not_found;
            l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_1_code;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            return;
            end if;
         end if;
/*     else                                               SLR - QC 1101
         if not dwh_valid.fnd_diff(g_rec_out.diff_1_code) then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_diff_not_found;
         l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_1_code;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
         end if;
      end if;    */
   end if;

   if g_rec_out.diff_2_code is not null then
 --     if g_rec_out.item_level_no < g_rec_out.tran_level_no then
         if not dwh_valid.fnd_diff_group(g_rec_out.diff_2_code) then
            if not dwh_valid.fnd_diff(g_rec_out.diff_2_code) then
            g_hospital      := 'Y';
            g_hospital_text := dwh_constants.vc_diff_not_found;
            l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_2_code;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            return;
            end if;
         end if;
 /*     else                                                     SLR - QC 1101
         if not dwh_valid.fnd_diff(g_rec_out.diff_2_code) then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_diff_not_found;
         l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_2_code;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
         end if;
      end if;     */
   end if;

   if g_rec_out.diff_3_code is not null then
 --     if g_rec_out.item_level_no < g_rec_out.tran_level_no then
         if not dwh_valid.fnd_diff_group(g_rec_out.diff_3_code) then
            if not dwh_valid.fnd_diff(g_rec_out.diff_3_code) then
            g_hospital      := 'Y';
            g_hospital_text := dwh_constants.vc_diff_not_found;
            l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_3_code;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            return;
            end if;
         end if;
 /*     else                                                   SLR - QC 1101
         if not dwh_valid.fnd_diff(g_rec_out.diff_3_code) then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_diff_not_found;
         l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_3_code;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
         end if;
      end if;  */
   end if;

   if g_rec_out.diff_4_code is not null then
 --     if g_rec_out.item_level_no < g_rec_out.tran_level_no then
         if not dwh_valid.fnd_diff_group(g_rec_out.diff_4_code) then
            if not dwh_valid.fnd_diff(g_rec_out.diff_4_code) then
            g_hospital      := 'Y';
            g_hospital_text := dwh_constants.vc_diff_not_found;
            l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_4_code;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            return;
            end if;
         end if;
 /*     else                                                   SLR - QC 1101
         if not dwh_valid.fnd_diff(g_rec_out.diff_4_code) then
         g_hospital      := 'Y';
         g_hospital_text := dwh_constants.vc_diff_not_found;
         l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_4_code;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
         end if;
      end if; */
   end if;

--   if g_rec_out.diff_1_code is not null then
--      if not dwh_valid.fnd_diff(g_rec_out.diff_1_code) then
--         g_hospital      := 'Y';
--         g_hospital_text := dwh_constants.vc_diff_not_found;
--         l_text          := dwh_constants.vc_diff_not_found||' '||g_rec_out.item_no||' '||g_rec_out.diff_1_code;
--         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--         return;
--      end if;
--   end if;



--  if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_invalid_source_code;
--     return;
--   end if;
   if g_rec_out.retail_zone_group_no is not null then
   if not dwh_valid.fnd_zone_group(g_rec_out.retail_zone_group_no) then
      g_hospital      := 'Y';
      g_hospital_text := dwh_constants.vc_zone_group_not_found;
      l_text          := dwh_constants.vc_zone_group_not_found||' '||g_rec_out.item_no||' '||g_rec_out.retail_zone_group_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      return;
   end if;
   end if; if g_rec_out.cost_zone_group_no is not null then
   if not dwh_valid.fnd_zone_group(g_rec_out.cost_zone_group_no) then
      g_hospital      := 'Y';
      g_hospital_text := dwh_constants.vc_zone_group_not_found;
      l_text          := dwh_constants.vc_zone_group_not_found||' '||g_rec_out.item_no||' '||g_rec_out.cost_zone_group_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      return;
   end if;
   end if;
-- Indicator checking to determine if 0(zero) or 1(one)

   if not dwh_valid.indicator_field(g_rec_out.primary_ref_item_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.rpl_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.item_aggr_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.diff_1_aggr_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.diff_2_aggr_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.diff_3_aggr_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.diff_4_aggr_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.merchandise_item_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.ext_sys_forecast_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.random_mass_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.constant_dimension_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.pack_item_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.pack_item_simple_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.pack_item_inner_pack_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.pack_item_sellable_unit_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.pack_item_orderable_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.gift_wrap_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.ship_alone_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.banded_item_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
     return;
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

   insert into stg_rms_item_hsp values g_rec_in;
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
       insert into fnd_item values a_tbl_insert(i);

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
       update fnd_item
       set   item_desc                       = a_tbl_update(i).item_desc,
             item_short_desc                 = a_tbl_update(i).item_short_desc,
             item_upper_desc                 = a_tbl_update(i).item_upper_desc,
             item_scndry_desc                = a_tbl_update(i).item_scndry_desc,
             item_status_code                = a_tbl_update(i).item_status_code,
             item_level_no                   = a_tbl_update(i).item_level_no,
             tran_level_no                   = a_tbl_update(i).tran_level_no,
             primary_ref_item_ind            = a_tbl_update(i).primary_ref_item_ind,
             item_parent_no                  = a_tbl_update(i).item_parent_no,
             item_grandparent_no             = a_tbl_update(i).item_grandparent_no,
             item_level1_no                  = a_tbl_update(i).item_level1_no,
             item_level2_no                  = a_tbl_update(i).item_level2_no,
             subclass_no                     = a_tbl_update(i).subclass_no,
             class_no                        = a_tbl_update(i).class_no,
             department_no                   = a_tbl_update(i).department_no,
             rpl_ind                         = a_tbl_update(i).rpl_ind,
             item_no_type                    = a_tbl_update(i).item_no_type,
             format_id                       = a_tbl_update(i).format_id,
             upc_prefix_no                   = a_tbl_update(i).upc_prefix_no,
             diff_1_code                     = a_tbl_update(i).diff_1_code,
             diff_2_code                     = a_tbl_update(i).diff_2_code,
             diff_3_code                     = a_tbl_update(i).diff_3_code,
             diff_4_code                     = a_tbl_update(i).diff_4_code,
             item_aggr_ind                   = a_tbl_update(i).item_aggr_ind,
             diff_1_aggr_ind                 = a_tbl_update(i).diff_1_aggr_ind,
             diff_2_aggr_ind                 = a_tbl_update(i).diff_2_aggr_ind,
             diff_3_aggr_ind                 = a_tbl_update(i).diff_3_aggr_ind,
             diff_4_aggr_ind                 = a_tbl_update(i).diff_4_aggr_ind,
             retail_zone_group_no            = a_tbl_update(i).retail_zone_group_no,
             cost_zone_group_no              = a_tbl_update(i).cost_zone_group_no,
             standard_uom_code               = a_tbl_update(i).standard_uom_code,
             standard_uom_desc               = a_tbl_update(i).standard_uom_desc,
             standard_uom_class_code         = a_tbl_update(i).standard_uom_class_code,
             uom_conv_factor                 = a_tbl_update(i).uom_conv_factor,
             package_size                    = a_tbl_update(i).package_size,
             package_uom_code                = a_tbl_update(i).package_uom_code,
             package_uom_desc                = a_tbl_update(i).package_uom_desc,
             package_uom_class_code          = a_tbl_update(i).package_uom_class_code,
             merchandise_item_ind            = a_tbl_update(i).merchandise_item_ind,
             store_ord_mult_unit_type_code   = a_tbl_update(i).store_ord_mult_unit_type_code,
             ext_sys_forecast_ind            = a_tbl_update(i).ext_sys_forecast_ind,
             primary_currency_original_rsp   = a_tbl_update(i).primary_currency_original_rsp,
             mfg_recommended_rsp             = a_tbl_update(i).mfg_recommended_rsp,
             retail_label_type               = a_tbl_update(i).retail_label_type,
             retail_label_value              = a_tbl_update(i).retail_label_value,
             handling_temp_code              = a_tbl_update(i).handling_temp_code,
             handling_sensitivity_code       = a_tbl_update(i).handling_sensitivity_code,
             random_mass_ind                 = a_tbl_update(i).random_mass_ind,
             first_received_date             = a_tbl_update(i).first_received_date,
             last_received_date              = a_tbl_update(i).last_received_date,
             most_recent_received_qty        = a_tbl_update(i).most_recent_received_qty,
             waste_type                      = a_tbl_update(i).waste_type,
             avg_waste_perc                  = a_tbl_update(i).avg_waste_perc,
             default_waste_perc              = a_tbl_update(i).default_waste_perc,
             constant_dimension_ind          = a_tbl_update(i).constant_dimension_ind,
             pack_item_ind                   = a_tbl_update(i).pack_item_ind,
             pack_item_simple_ind            = a_tbl_update(i).pack_item_simple_ind,
             pack_item_inner_pack_ind        = a_tbl_update(i).pack_item_inner_pack_ind,
             pack_item_sellable_unit_ind     = a_tbl_update(i).pack_item_sellable_unit_ind,
             pack_item_orderable_ind         = a_tbl_update(i).pack_item_orderable_ind,
             pack_item_type                  = a_tbl_update(i).pack_item_type,
             pack_item_receivable_type       = a_tbl_update(i).pack_item_receivable_type,
             item_comment                    = a_tbl_update(i).item_comment,
             item_service_level_type         = a_tbl_update(i).item_service_level_type,
             gift_wrap_ind                   = a_tbl_update(i).gift_wrap_ind,
             ship_alone_ind                  = a_tbl_update(i).ship_alone_ind,
             origin_item_ext_src_sys_name    = a_tbl_update(i).origin_item_ext_src_sys_name,
             banded_item_ind                 = a_tbl_update(i).banded_item_ind,
             static_mass                     = a_tbl_update(i).static_mass,
             ext_ref_id                      = a_tbl_update(i).ext_ref_id,
             create_date                     = a_tbl_update(i).create_date,
             size_id                         = a_tbl_update(i).size_id,
             color_id                        = a_tbl_update(i).color_id,
             style_colour_no                 = a_tbl_update(i).style_colour_no,
             style_no                        = a_tbl_update(i).style_no,
--             base_rsp                        = a_tbl_update(i).base_rsp,
             source_data_status_code         = a_tbl_update(i).source_data_status_code,
             primary_supplier_no             = a_tbl_update(i).primary_supplier_no,
             last_updated_date               = a_tbl_update(i).last_updated_date,
             FD_PRODUCT_NO                   = a_tbl_update(i).FD_PRODUCT_NO,
             PRODUCT_CLASS                   = a_tbl_update(i).PRODUCT_CLASS,
             FD_DISCIPLINE_TYPE              = a_tbl_update(i).FD_DISCIPLINE_TYPE,
             HANDLING_METHOD_CODE            = a_tbl_update(i).HANDLING_METHOD_CODE,
             HANDLING_METHOD_NAME            = a_tbl_update(i).HANDLING_METHOD_NAME,
             DISPLAY_METHOD_CODE             = a_tbl_update(i).DISPLAY_METHOD_CODE,
             DISPLAY_METHOD_NAME             = a_tbl_update(i).DISPLAY_METHOD_NAME,
             SEGREGATION_IND                 = a_tbl_update(i).SEGREGATION_IND,
             LOOSE_PROD_IND                  = a_tbl_update(i).LOOSE_PROD_IND,
             VAR_WEIGHT_IND                  = a_tbl_update(i).VAR_WEIGHT_IND
      where  item_no                         = a_tbl_update(i).item_no;

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
       update stg_rms_item_cpy
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
   g_found := dwh_valid.fnd_item(g_rec_out.item_no);

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).item_no = g_rec_out.item_no then
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
-- Check integrity
--**************************************************************************************************
procedure local_check_data_integrity as
begin

   l_text := 'Integrity check to ensure not more than 1 parent is present on relationship Style_colour to style to subclass.';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := '1st fiels are the correct, newly added records and last 3 are old previously added now with duplicate codes';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'To continue fix old codes to align to new and proceed to PRF level dim_item load (When you get to work at 8am!)';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'If no items display after this message then, you do not have a problem!!';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


-- Comment  -- Alastair de Wet ---   02/2009
-- Should this get thru to performance level it creats a problem of duplicate parents and crashes the cubes.
-- Also dim lev1 and lev1 diff1 are single parent entities and cannot hold both relationships.
-- Thus you will have 2 or more natural keys with the same surrogate key populated on dim item from dim lev1 diff1
-- Thus the run into prf layer must not continue until problem is fixed or we stuff up our tables
-- should we need to ever fix the problem then OLAP will need to rebuild as a hierachy relationship would be changing like a re-structure
-- ALWAYS ASSUME THAT THE OLD DATA ON FILE IS NOW INCORRECT AND FIX IT BEFOIRE GOING ONTO LOAD TO DIM_ITEM.
-- SHOULD THE LIST OF FIXES BE TOO LONG TO MANAGE/FIX THEN GET RMS INVOLVED OR IF THEY CANT HELP, THEN DIM_ITEM UPDATE WH_PRF_CORP_036A MUST BE
-- DISABLED NOT TO RUN WITHOUT MESSING UP THE SCHEDULE AND THEN DELETE WH_PRF_CORP_058U FROM THE SCHEDULE TO ALLOW THE RUN TO CONTINUE.
-- MAY NEED TO CALL IN APPWORX TO HELP DO THIS!!!
--

   for v_fnd_item in c_fnd_item
   loop
       g_recs_integ := 0;
       g_style_colour_no   := v_fnd_item.style_colour_no;
       g_style_no          := v_fnd_item.style_no;
       g_subclass_no       := v_fnd_item.subclass_no;
       G_CLASS_NO          := V_FND_ITEM.CLASS_NO;
       G_DEPARTMENT_NO     := V_FND_ITEM.DEPARTMENT_NO;

       for  v_fnd_item_old in c_fnd_item_old
       loop
          l_text := 'New - '||v_fnd_item.style_colour_no||'  '||v_fnd_item.style_no||'  '||v_fnd_item.subclass_no||'  '||v_fnd_item.class_no||
                    ' bad - '||v_fnd_item_old.style_colour_no||'  '||v_fnd_item_old.style_no||'  '||v_fnd_item_old.subclass_no||'  '||v_fnd_item_old.class_no||
                    ' item '||v_fnd_item_old.item_no;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          g_recs_rejected  := g_recs_rejected + 1;
-- AUTO UPDATE TO FIX THE ISSUE
        update fnd_item 
        set    style_no    = v_fnd_item.style_no,
               subclass_no = v_fnd_item.subclass_no,
               CLASS_NO    = V_FND_ITEM.CLASS_NO,
               DEPARTMENT_NO = V_FND_ITEM.DEPARTMENT_NO
        where  item_no     = v_fnd_item_old.item_no;
       end loop;
    end loop;
commit;

   for v_fnd_item_s in c_fnd_item_s
   loop
       g_recs_integ := 0;
       g_style_no          := v_fnd_item_s.style_no;
       g_subclass_no       := v_fnd_item_s.subclass_no;
       G_CLASS_NO          := V_FND_ITEM_S.CLASS_NO;
       G_DEPARTMENT_NO     := V_FND_ITEM_S.DEPARTMENT_NO;
       
       for  v_fnd_item_old_s in c_fnd_item_old_s
       loop
          l_text := 'New data - '||v_fnd_item_s.style_no||'  '||v_fnd_item_s.subclass_no||'  '||v_fnd_item_s.class_no||
                    ' Previous bad data - '||v_fnd_item_old_s.style_no||'  '||v_fnd_item_old_s.subclass_no||'  '||v_fnd_item_old_s.class_no||
                    ' for item '||v_fnd_item_old_s.item_no;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          g_recs_rejected  := g_recs_rejected + 1;
-- AUTO UPDATE TO FIX THE ISSUE
        update fnd_item 
        set    subclass_no = v_fnd_item_s.subclass_no,
               CLASS_NO    = V_FND_ITEM_S.CLASS_NO,
               DEPARTMENT_NO = V_FND_ITEM_S.DEPARTMENT_NO        
        where  item_no     = v_fnd_item_old_s.item_no;
       end loop;
    end loop;
commit;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    if g_recs_rejected > 0 then
       l_message := 'Apllication error - Duplicate parents see log file';
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise_application_error (-20500,'Application error - Duplicate parents see log files');
    end if;

   exception
      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_check_data_integrity;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 100  then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_ITEM EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    select restructure_ind
    into   g_restructure_ind
    from   dim_control;

    l_text := 'RESTRUCTURE_IND IS:- '||g_restructure_ind;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_item;
    fetch c_stg_rms_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_rms_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_item;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;

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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
--  Check integrity of style to style colour to subclass
--**************************************************************************************************
--    g_date := '06 jan 2009';  -- for testing
    local_check_data_integrity;


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

end wh_fnd_corp_058fix;
