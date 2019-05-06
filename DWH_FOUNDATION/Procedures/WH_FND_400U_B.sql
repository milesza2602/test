--------------------------------------------------------
--  DDL for Procedure WH_FND_400U_B
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_400U_B" (p_forall_limit in integer,p_success out boolean) as

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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_400U';
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




cursor c_fnd_item is
   select distinct style_colour_no,style_no,subclass_no
   from   fnd_item
   where  last_updated_date = g_date and
          item_level_no    >= tran_level_no ;
cursor c_fnd_item_old is
   select item_no,style_colour_no,style_no,subclass_no
   from   fnd_item
   where
--          last_updated_date < g_date and
-- This line checks only previous data. leaving it out checks all data
          style_colour_no =  g_style_colour_no and
          (style_no       <> g_style_no or
           subclass_no    <> g_subclass_no);

cursor c_fnd_item_s is
   select distinct style_no,subclass_no
   from   fnd_item
   where  last_updated_date = g_date and
          item_level_no    >= tran_level_no ;
cursor c_fnd_item_old_s is
   select item_no,style_no,subclass_no
   from   fnd_item
   where
           style_no       = g_style_no and
           subclass_no   <> g_subclass_no ;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';

   g_rec_out                                 := g_rec_in;

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
             last_updated_date               = a_tbl_update(i).last_updated_date
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

       for  v_fnd_item_old in c_fnd_item_old
       loop
          l_text := 'New data - '||v_fnd_item.style_colour_no||'  '||v_fnd_item.style_no||'  '||v_fnd_item.subclass_no||
                    ' Previous bad data - '||v_fnd_item_old.style_colour_no||'  '||v_fnd_item_old.style_no||'  '||v_fnd_item_old.subclass_no||
                    ' for item '||v_fnd_item_old.item_no;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          g_recs_rejected  := g_recs_rejected + 1;
       end loop;
    end loop;


   for v_fnd_item_s in c_fnd_item_s
   loop
       g_recs_integ := 0;
       g_style_no          := v_fnd_item_s.style_no;
       g_subclass_no       := v_fnd_item_s.subclass_no;

       for  v_fnd_item_old_s in c_fnd_item_old_s
       loop
          l_text := 'New data - '||v_fnd_item_s.style_no||'  '||v_fnd_item_s.subclass_no||
                    ' Previous bad data - '||v_fnd_item_old_s.style_no||'  '||v_fnd_item_old_s.subclass_no||
                    ' for item '||v_fnd_item_old_s.item_no;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          g_recs_rejected  := g_recs_rejected + 1;

       end loop;
    end loop;

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
-- Derive which records are deltas - Hope to rather get deltas sent to us - Waiting for RMS
--**************************************************************************************************

procedure get_deltas as
begin

insert into dwh_foundation.temp_stg_item_deltas
with selext 
as(   
select item_no, max(sys_source_batch_id||sys_source_sequence_no) maxval
   from stg_rms_item_ARC
WHERE SYS_LOAD_DATE = '18 FEB 2013'
   group by item_no
   )
 select 
 ARC.ITEM_NO
, ARC.ITEM_DESC
, ARC.ITEM_SHORT_DESC
, ARC.ITEM_UPPER_DESC
, ARC.ITEM_SCNDRY_DESC
, ARC.ITEM_STATUS_CODE
, ARC.ITEM_LEVEL_NO
, ARC.TRAN_LEVEL_NO
, ARC.PRIMARY_REF_ITEM_IND
, ARC.ITEM_PARENT_NO
, ARC.ITEM_GRANDPARENT_NO
, ARC.ITEM_LEVEL1_NO
, ARC.ITEM_LEVEL2_NO
, ARC.SUBCLASS_NO
, ARC.CLASS_NO
, ARC.DEPARTMENT_NO
, ARC.RPL_IND
, ARC.ITEM_NO_TYPE
, ARC.FORMAT_ID
, ARC.UPC_PREFIX_NO
, ARC.DIFF_1_CODE
, ARC.DIFF_2_CODE
, ARC.DIFF_3_CODE
, ARC.DIFF_4_CODE
, ARC.ITEM_AGGR_IND
, ARC.DIFF_1_AGGR_IND
, ARC.DIFF_2_AGGR_IND
, ARC.DIFF_3_AGGR_IND
, ARC.DIFF_4_AGGR_IND
, ARC.RETAIL_ZONE_GROUP_NO
, ARC.COST_ZONE_GROUP_NO
, ARC.STANDARD_UOM_CODE
, ARC.STANDARD_UOM_DESC
, ARC.STANDARD_UOM_CLASS_CODE
, ARC.UOM_CONV_FACTOR
, ARC.PACKAGE_SIZE
, ARC.PACKAGE_UOM_CODE
, ARC.PACKAGE_UOM_DESC
, ARC.PACKAGE_UOM_CLASS_CODE
, ARC.MERCHANDISE_ITEM_IND
, ARC.STORE_ORD_MULT_UNIT_TYPE_CODE
, ARC.EXT_SYS_FORECAST_IND
, ARC.PRIMARY_CURRENCY_ORIGINAL_RSP
, ARC.MFG_RECOMMENDED_RSP
, ARC.RETAIL_LABEL_TYPE
, ARC.RETAIL_LABEL_VALUE
, ARC.HANDLING_TEMP_CODE
, ARC.HANDLING_SENSITIVITY_CODE
, ARC.RANDOM_MASS_IND
, ARC.FIRST_RECEIVED_DATE
, ARC.LAST_RECEIVED_DATE
, ARC.MOST_RECENT_RECEIVED_QTY
, ARC.WASTE_TYPE
, ARC.AVG_WASTE_PERC
, ARC.DEFAULT_WASTE_PERC
, ARC.CONSTANT_DIMENSION_IND
, ARC.PACK_ITEM_IND
, ARC.PACK_ITEM_SIMPLE_IND
, ARC.PACK_ITEM_INNER_PACK_IND
, ARC.PACK_ITEM_SELLABLE_UNIT_IND
, ARC.PACK_ITEM_ORDERABLE_IND
, ARC.PACK_ITEM_TYPE
, ARC.PACK_ITEM_RECEIVABLE_TYPE
, ARC.ITEM_COMMENT
, ARC.ITEM_SERVICE_LEVEL_TYPE
, ARC.GIFT_WRAP_IND
, ARC.SHIP_ALONE_IND
, ARC.ORIGIN_ITEM_EXT_SRC_SYS_NAME
, ARC.BANDED_ITEM_IND
, ARC.STATIC_MASS
, ARC.EXT_REF_ID
, ARC.CREATE_DATE
, ARC.SIZE_ID
, ARC.COLOR_ID
, ARC.STYLE_COLOUR_NO
, ARC.STYLE_NO
, ARC.SOURCE_DATA_STATUS_CODE
, ARC.PRIMARY_SUPPLIER_NO
   from stg_rms_item_ARC ARC, selext se
   where ARC.sys_source_batch_id||sys_source_sequence_no = se.maxval
AND SYS_LOAD_DATE = '18 FEB 2013'
 minus
       select 
 fi.ITEM_NO
, fi.ITEM_DESC
, fi.ITEM_SHORT_DESC
, fi.ITEM_UPPER_DESC
, fi.ITEM_SCNDRY_DESC
, fi.ITEM_STATUS_CODE
, fi.ITEM_LEVEL_NO
, fi.TRAN_LEVEL_NO
, fi.PRIMARY_REF_ITEM_IND
, fi.ITEM_PARENT_NO
, fi.ITEM_GRANDPARENT_NO
, fi.ITEM_LEVEL1_NO
, fi.ITEM_LEVEL2_NO
, fi.SUBCLASS_NO
, fi.CLASS_NO
, fi.DEPARTMENT_NO
, fi.RPL_IND
, fi.ITEM_NO_TYPE
, fi.FORMAT_ID
, fi.UPC_PREFIX_NO
, fi.DIFF_1_CODE
, fi.DIFF_2_CODE
, fi.DIFF_3_CODE
, fi.DIFF_4_CODE
, fi.ITEM_AGGR_IND
, fi.DIFF_1_AGGR_IND
, fi.DIFF_2_AGGR_IND
, fi.DIFF_3_AGGR_IND
, fi.DIFF_4_AGGR_IND
, fi.RETAIL_ZONE_GROUP_NO
, fi.COST_ZONE_GROUP_NO
, fi.STANDARD_UOM_CODE
, fi.STANDARD_UOM_DESC
, fi.STANDARD_UOM_CLASS_CODE
, fi.UOM_CONV_FACTOR
, fi.PACKAGE_SIZE
, fi.PACKAGE_UOM_CODE
, fi.PACKAGE_UOM_DESC
, fi.PACKAGE_UOM_CLASS_CODE
, fi.MERCHANDISE_ITEM_IND
, fi.STORE_ORD_MULT_UNIT_TYPE_CODE
, fi.EXT_SYS_FORECAST_IND
, fi.PRIMARY_CURRENCY_ORIGINAL_RSP
, fi.MFG_RECOMMENDED_RSP
, fi.RETAIL_LABEL_TYPE
, fi.RETAIL_LABEL_VALUE
, fi.HANDLING_TEMP_CODE
, fi.HANDLING_SENSITIVITY_CODE
, fi.RANDOM_MASS_IND
, fi.FIRST_RECEIVED_DATE
, fi.LAST_RECEIVED_DATE
, fi.MOST_RECENT_RECEIVED_QTY
, fi.WASTE_TYPE
, fi.AVG_WASTE_PERC
, fi.DEFAULT_WASTE_PERC
, fi.CONSTANT_DIMENSION_IND
, fi.PACK_ITEM_IND
, fi.PACK_ITEM_SIMPLE_IND
, fi.PACK_ITEM_INNER_PACK_IND
, fi.PACK_ITEM_SELLABLE_UNIT_IND
, fi.PACK_ITEM_ORDERABLE_IND
, fi.PACK_ITEM_TYPE
, fi.PACK_ITEM_RECEIVABLE_TYPE
, fi.ITEM_COMMENT
, fi.ITEM_SERVICE_LEVEL_TYPE
, fi.GIFT_WRAP_IND
, fi.SHIP_ALONE_IND
, fi.ORIGIN_ITEM_EXT_SRC_SYS_NAME
, fi.BANDED_ITEM_IND
, fi.STATIC_MASS
, fi.EXT_REF_ID
, fi.CREATE_DATE
, fi.SIZE_ID
, fi.COLOR_ID
, fi.STYLE_COLOUR_NO
, fi.STYLE_NO
, fi.SOURCE_DATA_STATUS_CODE
, fi.PRIMARY_SUPPLIER_NO
from fnd_item fi;

g_cnt := sql%rowcount;

       l_text          := 'No. of deltas INSERTED into temp_STG_item_deltas = '||g_cnt  ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

commit;

   exception
      when others then
       l_message := dwh_constants.vc_err_other||' get_deltas '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end get_deltas;

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

--
-- Look up batch date from dim_control
--
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--
--  Create backup of FND_ITEM
--
    l_text := 'truncate table dwh_foundation.temp_fnd_item_backup';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute IMMEDIATELY('truncate table dwh_foundation.temp_fnd_item_backup');
    l_text := 'truncate table dwh_foundation.temp_fnd_item_backup';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    execute IMMEDIATELY('insert append dwh_foundation.temp_fnd_item_backup select * from dwh_foundation.fnd_item');
    commit;
    select COUNT(*) into G_CNTB from DWH_FOUNDATION.TEMP_FND_ITEM_BACKUP;
    select COUNT(*) into G_CNT from DWH_FOUNDATION.FND_ITEM;  
    if G_CNTB <> G_CNT
    then
        L_TEXT := 'problem with creating backup - cannot continue - ABORTING';
        DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
        select g_cnt/0 from dual;  -- will force abort
    end if;
    
--
--  Derive DELTAS
--
    l_text := 'truncate table dwh_foundation.temp_stg_item_deltas';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute IMMEDIATELY('truncate table dwh_foundation.temp_stg_item_deltas');
    
    Get_deltas;




--**************************************************************************************************
-- Look up restructure_ind from dim_control
--**************************************************************************************************
    select restructure_ind
    into   g_restructure_ind
    from   dim_control;

    l_text := 'RESTRUCTURE_IND IS:- '||g_restructure_ind;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
   insert append into fnd_item
 select 
 ARC.ITEM_NO
, ARC.ITEM_DESC
, ARC.ITEM_SHORT_DESC
, ARC.ITEM_UPPER_DESC
, ARC.ITEM_SCNDRY_DESC
, ARC.ITEM_STATUS_CODE
, ARC.ITEM_LEVEL_NO
, ARC.TRAN_LEVEL_NO
, ARC.PRIMARY_REF_ITEM_IND
, ARC.ITEM_PARENT_NO
, ARC.ITEM_GRANDPARENT_NO
, ARC.ITEM_LEVEL1_NO
, ARC.ITEM_LEVEL2_NO
, ARC.SUBCLASS_NO
, ARC.CLASS_NO
, ARC.DEPARTMENT_NO
, ARC.RPL_IND
, ARC.ITEM_NO_TYPE
, ARC.FORMAT_ID
, ARC.UPC_PREFIX_NO
, ARC.DIFF_1_CODE
, ARC.DIFF_2_CODE
, ARC.DIFF_3_CODE
, ARC.DIFF_4_CODE
, ARC.ITEM_AGGR_IND
, ARC.DIFF_1_AGGR_IND
, ARC.DIFF_2_AGGR_IND
, ARC.DIFF_3_AGGR_IND
, ARC.DIFF_4_AGGR_IND
, ARC.RETAIL_ZONE_GROUP_NO
, ARC.COST_ZONE_GROUP_NO
, ARC.STANDARD_UOM_CODE
, ARC.STANDARD_UOM_DESC
, ARC.STANDARD_UOM_CLASS_CODE
, ARC.UOM_CONV_FACTOR
, ARC.PACKAGE_SIZE
, ARC.PACKAGE_UOM_CODE
, ARC.PACKAGE_UOM_DESC
, ARC.PACKAGE_UOM_CLASS_CODE
, ARC.MERCHANDISE_ITEM_IND
, ARC.STORE_ORD_MULT_UNIT_TYPE_CODE
, ARC.EXT_SYS_FORECAST_IND
, ARC.PRIMARY_CURRENCY_ORIGINAL_RSP
, ARC.MFG_RECOMMENDED_RSP
, ARC.RETAIL_LABEL_TYPE
, ARC.RETAIL_LABEL_VALUE
, ARC.HANDLING_TEMP_CODE
, ARC.HANDLING_SENSITIVITY_CODE
, ARC.RANDOM_MASS_IND
, ARC.FIRST_RECEIVED_DATE
, ARC.LAST_RECEIVED_DATE
, ARC.MOST_RECENT_RECEIVED_QTY
, ARC.WASTE_TYPE
, ARC.AVG_WASTE_PERC
, ARC.DEFAULT_WASTE_PERC
, ARC.CONSTANT_DIMENSION_IND
, ARC.PACK_ITEM_IND
, ARC.PACK_ITEM_SIMPLE_IND
, ARC.PACK_ITEM_INNER_PACK_IND
, ARC.PACK_ITEM_SELLABLE_UNIT_IND
, ARC.PACK_ITEM_ORDERABLE_IND
, ARC.PACK_ITEM_TYPE
, ARC.PACK_ITEM_RECEIVABLE_TYPE
, ARC.ITEM_COMMENT
, ARC.ITEM_SERVICE_LEVEL_TYPE
, ARC.GIFT_WRAP_IND
, ARC.SHIP_ALONE_IND
, ARC.ORIGIN_ITEM_EXT_SRC_SYS_NAME
, ARC.BANDED_ITEM_IND
, ARC.STATIC_MASS
, ARC.EXT_REF_ID
, ARC.CREATE_DATE
, ARC.SIZE_ID
, ARC.COLOR_ID
, (case ITEM_LEVEL_NO < TRAN_LEVEL_NO then NVL(STYLE_NO,0) else STYLE_NO end) STYLE_NO
, (case item_level_no < tran_level_no then nvl(style_colour_no,0) else style_colour_no end) style_colour_no
, ARC.SOURCE_DATA_STATUS_CODE
, ARC.PRIMARY_SUPPLIER_NO
FROM dwh_foundation.TEMP_STG_ITEM_DELTAS CPY,
  fnd_diff_group FDG1,
  FND_DIFF FD1 
  fnd_diff_group FDG2,
  FND_DIFF FD2 
  fnd_diff_group FDG3,
  FND_DIFF FD3 
  fnd_diff_group FDG4,
  fnd_diff FD4,
  fnd_supplier fs,
  fnd_zone_group fzg1,
  FND_ZONE_GROUP FZG2,
  FND_SUBCLASS FSC,
  FND_ITEM FI
where  
CPY.diff_1_code          = FDG1.DIff_group_code(+)
and CPY.DIFF_1_CODE          = FD1.DIFF_GROUP(+) 
and CPY.diff_1_code = FDG2.DIff_group_code(+)
and CPY.DIFF_1_CODE          = FD2.DIFF_GROUP(+) 
and CPY.diff_1_code = FDG3.DIff_group_code(+)
and CPY.DIFF_1_CODE          = FD3.DIFF_GROUP(+) 
and CPY.DIFF_1_CODE = FDG4.DIFF_GROUP_CODE(+)
and CPY.DIFF_1_CODE          = FD4.DIFF_GROUP(+)
and (case when FDG1.DIFF_GROUP_CODE is not null then 1 else 0 end) > 0
and  (case when FD1.DIFF_GROUP IS not null then 1 else 0 end) > 0
and   (case when FDG2.DIFF_GROUP_CODE is not null then 1 else 0 end) > 0
and  (case when FD2.DIFF_GROUP IS not null then 1 else 0 end) > 0
and   (case when FDG3.DIFF_GROUP_CODE is not null then 1 else 0 end) > 0
and  (case when FD3.DIFF_GROUP IS not null then 1 else 0 end) > 0
and   (case when FDG4.DIFF_GROUP_CODE is not null then 1 else 0 end) > 0
and  (case when FD4.DIff_group is not null then 1 else 0 end) > 0

and FS.SUPPLIER_NO           = CPY.PRIMARY_SUPPLIER_NO(+)
AND (case when nvl(FS.SUPPLIER_NO,0) >= 0 then 1 else 0 end) > 0
 
and CPY.RETAIL_ZONE_GROUP_NO = FZG1.ZONE_GROUP_NO(+)
and  (case when FZG1.ZONE_GROUP_NO is not null then 1 else 0 end) > 0

and CPY.COST_ZONE_GROUP_NO   = FZG2.ZONE_GROUP_NO(+)
and  (case when FZG2.ZONE_GROUP_NO is not null then 1 else 0 end) > 0


and CPY.SUBCLASS_NO = FSC.SUBCLASS_NO(+)
and CPY.CLASS_NO = FSC.CLASS_NO(+)
and CPY.DEPARTMENT_NO = FSC.DEPARTMENT_NO(+)
and  (case when FSC.SUBCLASS_NO||FSC.CLASS_NO||FSC.DEPARTMENT_NO is not null then 1 else 0 end) > 0

and CPY.item_NO = FI.item_NO(+)
and  (case when F1.SUBCLASS_NO||F1.CLASS_NO||F1.DEPARTMENT_NO||FI.STYLE_NO||FI.STYLE_COLOUR_NO is not null 
            and F1.SUBCLASS_NO||F1.CLASS_NO||F1.DEPARTMENT_NO||FI.STYLE_NO||FI.STYLE_COLOUR_NO = 
                CPY.SUBCLASS_NO||CPY.CLASS_NO||CPY.DEPARTMENT_NO||CPY.STYLE_NO||CPY.STYLE_COLOUR_NO 
            AND g_restructure_ind = 0 then 1 else 0 end) > 0

and  (case when PRIMARY_REF_ITEM_IND is null  then 1 when PRIMARY_REF_ITEM_IND in(0,1) then 1 else 0 end) > 0
and  (case when RPL_IND is null  then 1 when RPL_IND in(0,1) then 1 else 0 end) > 0
and  (case when ITEM_AGGR_IND is null  then 1 when ITEM_AGGR_IND in(0,1) then 1 else 0 end) > 0
and  (case when DIFF_1_AGGR_IND is null  then 1 when DIFF_1_AGGR_IND in(0,1) then 1 else 0 end) > 0
and  (case when DIFF_2_AGGR_IND is null  then 1 when DIFF_2_AGGR_IND in(0,1) then 1 else 0 end) > 0
and  (case when DIFF_3_AGGR_IND is null  then 1 when DIFF_3_AGGR_IND in(0,1) then 1 else 0 end) > 0
and  (case when DIFF_4_AGGR_IND is null  then 1 when DIFF_4_AGGR_IND in(0,1) then 1 else 0 end) > 0
and  (case when MERCHANDISE_ITEM_IND is null  then 1 when MERCHANDISE_ITEM_IND in(0,1) then 1 else 0 end) > 0
and  (case when EXT_SYS_FORECAST_IND is null  then 1 when EXT_SYS_FORECAST_IND in(0,1) then 1 else 0 end) > 0
and  (case when RANDOM_MASS_IND is null  then 1 when RANDOM_MASS_IND in(0,1) then 1 else 0 end) > 0
and  (case when CONSTANT_DIMENSION_IND is null  then 1 when CONSTANT_DIMENSION_IND in(0,1) then 1 else 0 end) > 0
and  (case when PACK_ITEM_IN is null  then 1 when PACK_ITEM_IN in(0,1) then 1 else 0 end) > 0
and  (case when PACK_ITEM_INNER_PACK_IND is null  then 1 when PACK_ITEM_INNER_PACK_IND in(0,1) then 1 else 0 end) > 0
and  (case when PACK_ITEM_SELLABLE_UNIT_IND is null  then 1 when PACK_ITEM_SELLABLE_UNIT_IND in(0,1) then 1 else 0 end) > 0
and  (case when PACK_ITEM_ORDERABLE_IND is null  then 1 when PACK_ITEM_ORDERABLE_IND in(0,1) then 1 else 0 end) > 0
and  (case when GIFT_WRAP_IND is null  then 1 when GIFT_WRAP_IND in(0,1) then 1 else 0 end) > 0
and  (case when SHIP_ALONE_IND is null  then 1 when SHIP_ALONE_IND in(0,1) then 1 else 0 end) > 0
and  (case when BANDED_ITEM_IND is null  then 1 when BANDED_ITEM_IND in(0,1) then 1 else 0 end) > 0

and  (case when ITEM_LEVEL_NO >= TRAN_LEVEL_NO  and (STYLE_NO is null  or STYLE_NO = 0) then 0 else 1 end) > 0
and  (case when ITEM_LEVEL_NO >= TRAN_LEVEL_NO  and (STYLE_COLOUR_NO is null  or STYLE_COLOUR_NO = 0) then 0 else 1 end) > 0         
  ;
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


END WH_FND_400U_B;
