--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_036B
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_036B" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Item master data table in the performance layer
--               with added value ex foundation layer DIFF tables and primary supplier and supply chain type.
--  Tables:      Input  - dim_item, dim_diff range of tables
--               Output - dim_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  18 Feb 2009 - defect 737 - Rename fields most_recent_MERCH_SEASON_NO
--                             and most_recent_MERCH_PHASE_NO to
--                             MOST_RECENT_MERCH_SEASON_NO and
--                             MOST_RECENT_MERCH_PHASE_NO
--                             on tables DIM_ITEM, DIM_ITEM_HIST
--                             and DIM_LEV1_DIFF1
--
--  18 March 2009 - defect 1153 - Replace NULLs with 'standard' values for some
--                                Performance layer Dimension table attributes.
--                              - columns which need extra checks other than
--                                 null to be added are :
--                                 DIFF_1_CODE;
--                                 DIFF_2_CODE;
--                                 DIFF_3_CODE;
--                                 DIFF_4_CODE;
-- 3 April 2009 - defect 1181 - All cubes: Item level 1 Description not comming
--                             through : displaying as Item level 1 Description?
-- 28 April - defect 1523 - Remove hardcoding in selection for FND_ITEM
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_item%rowtype;
g_diff_code          dim_item.diff_1_code%type;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_location_no        dim_location.location_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_036B';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_ITEM EX DIM_ITEM & DIFF TABLES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_item%rowtype index by binary_integer;
type tbl_array_u is table of dim_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

--
-- Need to update the item_level1_desc by joinging back to dim_item
--    item_level1_no joined to fi.item_level1_no
--
cursor c_dim_item is
   with
     sel_lev1 as
     (select di1.item_level1_no,
             nvl((trim(di2.item_upper_desc)),'No Value') item_upper_desc
      from fnd_item di1,
           fnd_item di2
      where di1.item_level1_no = di2.item_no
      group by di1.item_level1_no,
               di2.item_upper_desc)
      select fi.item_no,
          fi.diff_1_code,
          fi.diff_2_code,
          fi.diff_3_code,
          fi.diff_4_code,
          fi.item_level1_no,
          fi.item_level_no,
          fi.tran_level_no,
          sl.item_upper_desc item_level1_desc
   from   fnd_item fi,
          sel_lev1 sl
   where fi.item_level1_no = sl.item_level1_no(+)
--   and sl.item_upper_desc like 'MOCC SLIPPER'
   order by fi.item_no;



-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

g_rec_in             c_dim_item%rowtype;
-- For input bulk collect --
type stg_array is table of c_dim_item%rowtype;
a_stg_input      stg_array;




--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.SK1_SUPPLIER_NO           := '0';
   g_rec_out.PRIMARY_SUPPLIER_NO       := '99555';
   g_rec_out.SK1_MERCH_SEASON_PHASE_NO := '0';
   g_rec_out.most_recent_MERCH_SEASON_NO  := '0';
   g_rec_out.most_recent_MERCH_PHASE_NO   := '0';
   g_rec_out.DIFF_1_CODE_DESC          := '-';
   g_rec_out.DIFF_2_CODE_DESC          := '-';
   g_rec_out.DIFF_3_CODE_DESC          := '-';
   g_rec_out.DIFF_4_CODE_DESC          := '-';
   g_rec_out.DIFF_1_DIFF_TYPE          := '-';
   g_rec_out.DIFF_2_DIFF_TYPE          := '-';
   g_rec_out.DIFF_3_DIFF_TYPE          := '-';
   g_rec_out.DIFF_4_DIFF_TYPE          := '-';
   g_rec_out.DIFF_1_TYPE_DESC          := '-';
   g_rec_out.DIFF_2_TYPE_DESC          := '-';
   g_rec_out.DIFF_3_TYPE_DESC          := '-';
   g_rec_out.DIFF_4_TYPE_DESC          := '-';
   g_rec_out.DIFF_1_DIFF_GROUP_CODE    := '-';
   g_rec_out.DIFF_2_DIFF_GROUP_CODE    := '-';
   g_rec_out.DIFF_3_DIFF_GROUP_CODE    := '-';
   g_rec_out.DIFF_4_DIFF_GROUP_CODE    := '-';
   g_rec_out.DIFF_1_DIFF_GROUP_DESC    := '-';
   g_rec_out.DIFF_2_DIFF_GROUP_DESC    := '-';
   g_rec_out.DIFF_3_DIFF_GROUP_DESC    := '-';
   g_rec_out.DIFF_4_DIFF_GROUP_DESC    := '-';
   g_rec_out.DIFF_1_DISPLAY_SEQ        := 0;
   g_rec_out.DIFF_2_DISPLAY_SEQ        := 0;
   g_rec_out.DIFF_3_DISPLAY_SEQ        := 0;
   g_rec_out.DIFF_4_DISPLAY_SEQ        := 0;
   g_rec_out.DIFF_TYPE_COLOUR_DIFF_CODE      := '-';
   g_rec_out.DIFF_TYPE_PRIM_SIZE_DIFF_CODE   := '-';
   g_rec_out.DIFF_TYPE_SCND_SIZE_DIFF_CODE   := '-';
   g_rec_out.DIFF_TYPE_FRAGRANCE_DIFF_CODE   := '-';

   g_rec_out.item_level1_desc := g_rec_in.item_level1_desc;
   g_rec_out.item_level1_long_desc := substr((g_rec_in.item_level1_no||' - '||g_rec_out.item_level1_desc),1,100);

   g_rec_out.last_updated_date               := g_date;
   g_rec_out.item_no                         := g_rec_in.item_no;

/*
   QC2475  No longer need to look up primary supplier
   begin
      select supplier_no
      into   g_rec_out.primary_supplier_no
      from   fnd_item_supplier
      where  item_no              = g_rec_out.item_no and
             primary_supplier_ind = 1 and
             rownum = 1;

      select sk1_supplier_no
      into   g_rec_out.sk1_supplier_no
      from   dim_supplier
      where  supplier_no = g_rec_out.primary_supplier_no;

      exception
         when no_data_found then
                 select sk1_supplier_no
                 into   g_rec_out.sk1_supplier_no
                 from   dim_supplier
                 where  supplier_no = g_rec_out.primary_supplier_no;
         when others then
            l_message := 'fnd_supplier_Item  or dim_supplier lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
   end;
*/
   if g_rec_in.diff_1_code is not null
   and g_rec_in.diff_1_code <> '-'
   then
   begin
      select DIFF_CODE_DESC,
             DIFF_TYPE,
             DIFF_TYPE_DESC
      into   g_rec_out.DIFF_1_CODE_DESC,
             g_rec_out.DIFF_1_DIFF_TYPE,
             g_rec_out.DIFF_1_TYPE_DESC
      from   fnd_diff
      where  diff_code        = g_rec_in.diff_1_code;

      exception
         when no_data_found then
           null;
         when others then
            l_message := 'fnd_diff diff_1 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
   end;
   end if;

   if g_rec_in.diff_2_code is not null
      and g_rec_in.diff_2_code <> '-'
   then
   begin
      select DIFF_CODE_DESC,
             DIFF_TYPE,
             DIFF_TYPE_DESC
      into   g_rec_out.DIFF_2_CODE_DESC,
             g_rec_out.DIFF_2_DIFF_TYPE,
             g_rec_out.DIFF_2_TYPE_DESC
      from   fnd_diff
      where  diff_code        = g_rec_in.diff_2_code;

      exception
         when no_data_found then
           null;
         when others then
            l_message := 'fnd_diff diff_2 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
   end;
   end if;

   if g_rec_in.diff_3_code is not null
      and g_rec_in.diff_3_code <> '-'
   then
   begin
      select DIFF_CODE_DESC,
             DIFF_TYPE,
             DIFF_TYPE_DESC
      into   g_rec_out.DIFF_3_CODE_DESC,
             g_rec_out.DIFF_3_DIFF_TYPE,
             g_rec_out.DIFF_3_TYPE_DESC
      from   fnd_diff
      where  diff_code        = g_rec_in.diff_3_code;

      exception
         when no_data_found then
           null;
         when others then
            l_message := 'fnd_diff diff_3 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
   end;
   end if;

   if g_rec_in.diff_4_code is not null
      and g_rec_in.diff_4_code <> '-'
   then
   begin
      select DIFF_CODE_DESC,
             DIFF_TYPE,
             DIFF_TYPE_DESC
      into   g_rec_out.DIFF_4_CODE_DESC,
             g_rec_out.DIFF_4_DIFF_TYPE,
             g_rec_out.DIFF_4_TYPE_DESC
      from   fnd_diff
      where  diff_code        = g_rec_in.diff_4_code;

      exception
         when no_data_found then
           null;
         when others then
            l_message := 'fnd_diff diff_4 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
   end;
   end if;

   case g_rec_out.diff_1_diff_type
      when 'C' then
         g_rec_out.diff_type_colour_diff_code        := g_rec_in.diff_1_code;
      when 'PS' then
         g_rec_out.diff_type_prim_size_diff_code     := g_rec_in.diff_1_code;
      when 'SS' then
         g_rec_out.diff_type_scnd_size_diff_code     := g_rec_in.diff_1_code;
      when 'FR' then
         g_rec_out.diff_type_fragrance_diff_code     := g_rec_in.diff_1_code;
      else
         null;
   end case;

   case g_rec_out.diff_2_diff_type
      when 'C' then
         g_rec_out.diff_type_colour_diff_code        := g_rec_in.diff_2_code;
      when 'PS' then
         g_rec_out.diff_type_prim_size_diff_code     := g_rec_in.diff_2_code;
      when 'SS' then
         g_rec_out.diff_type_scnd_size_diff_code     := g_rec_in.diff_2_code;
      when 'FR' then
         g_rec_out.diff_type_fragrance_diff_code     := g_rec_in.diff_2_code;
      else
         null;
   end case;

   case g_rec_out.diff_3_diff_type
      when 'C' then
         g_rec_out.diff_type_colour_diff_code        := g_rec_in.diff_3_code;
      when 'PS' then
         g_rec_out.diff_type_prim_size_diff_code     := g_rec_in.diff_3_code;
      when 'SS' then
         g_rec_out.diff_type_scnd_size_diff_code     := g_rec_in.diff_3_code;
      when 'FR' then
         g_rec_out.diff_type_fragrance_diff_code     := g_rec_in.diff_3_code;
      else
         null;
   end case;

   case g_rec_out.diff_4_diff_type
      when 'C' then
         g_rec_out.diff_type_colour_diff_code        := g_rec_in.diff_4_code;
      when 'PS' then
         g_rec_out.diff_type_prim_size_diff_code     := g_rec_in.diff_4_code;
      when 'SS' then
         g_rec_out.diff_type_scnd_size_diff_code     := g_rec_in.diff_4_code;
      when 'FR' then
         g_rec_out.diff_type_fragrance_diff_code     := g_rec_in.diff_4_code;
      else
         null;
   end case;

   if g_rec_in.item_level_no >= g_rec_in.tran_level_no  then


      if g_rec_in.diff_1_code is not null
         and g_rec_in.diff_1_code <> '-'
       then
       begin
         select diff_1_code
         into   g_diff_code
         from   dim_item
         where  item_no = g_rec_in.item_level1_no;

         select diff_group_code, diff_group_desc
         into   g_rec_out.diff_1_diff_group_code, g_rec_out.diff_1_diff_group_desc
         from   fnd_diff_group
         where  diff_group_code = g_diff_code;

         select display_seq
         into   g_rec_out.diff_1_display_seq
         from   fnd_diff_group_detail
         where  diff_group_code = g_diff_code and
                diff_code       = g_rec_in.diff_1_code;

         exception
         when no_data_found then
           null;
         when others then
            l_message := 'Diff group 1 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
       end;
       end if;
      if g_rec_in.diff_2_code is not null
         and g_rec_in.diff_2_code <> '-'
       then
       begin
         select diff_2_code
         into   g_diff_code
         from   dim_item
         where  item_no = g_rec_in.item_level1_no;

         select diff_group_code, diff_group_desc
         into   g_rec_out.diff_2_diff_group_code, g_rec_out.diff_2_diff_group_desc
         from   fnd_diff_group
         where  diff_group_code = g_diff_code;

         select display_seq
         into   g_rec_out.diff_2_display_seq
         from   fnd_diff_group_detail
         where  diff_group_code = g_diff_code and
                diff_code       = g_rec_in.diff_2_code;

         exception
         when no_data_found then
           null;
         when others then
            l_message := 'Diff group 2 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
       end;
       end if;
       if g_rec_in.diff_3_code is not null
         and g_rec_in.diff_3_code <> '-'
       then
       begin
         select diff_3_code
         into   g_diff_code
         from   dim_item
         where  item_no = g_rec_in.item_level1_no;

         select diff_group_code, diff_group_desc
         into   g_rec_out.diff_3_diff_group_code, g_rec_out.diff_3_diff_group_desc
         from   fnd_diff_group
         where  diff_group_code = g_diff_code;

         select display_seq
         into   g_rec_out.diff_3_display_seq
         from   fnd_diff_group_detail
         where  diff_group_code = g_diff_code and
                diff_code       = g_rec_in.diff_3_code;

         exception
         when no_data_found then
           null;
         when others then
            l_message := 'Diff group 3 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
       end;
       end if;
      if g_rec_in.diff_4_code is not null
         and g_rec_in.diff_4_code <> '-'
       then
       begin
         select diff_4_code
         into   g_diff_code
         from   dim_item
         where  item_no = g_rec_in.item_level1_no;

         select diff_group_code, diff_group_desc
         into   g_rec_out.diff_4_diff_group_code, g_rec_out.diff_4_diff_group_desc
         from   fnd_diff_group
         where  diff_group_code = g_diff_code;

         select display_seq
         into   g_rec_out.diff_4_display_seq
         from   fnd_diff_group_detail
         where  diff_group_code = g_diff_code and
                diff_code       = g_rec_in.diff_4_code;

         exception
         when no_data_found then
           null;
         when others then
            l_message := 'Diff group 4 lookup failure '||sqlcode||' '||sqlerrm;
            dwh_log.record_error(l_module_name,sqlcode,l_message);
            raise;
       end;
       end if;
   end if;
---------------------------------------------------------
-- Add supply chain type   Removed due to overhead
---------------------------------------------------------
/*   begin
      select lisc.location_no
      into   g_location_no
      from   fnd_loc_item_supplier_country lisc, fnd_location loc
      where  item_no          = g_rec_out.item_no and
             supplier_no      = g_rec_out.primary_supplier_no and
             supplier_delivery_loc_ind = 1 and
             lisc.location_no = loc.location_no and
             loc.loc_type     = 'W' and
             rownum = 1;
      exception
        when no_data_found then
           g_location_no := 0;
   end;
   begin
      select supply_chain_type
      into   g_rec_out.supply_chain_type
      from   fnd_location_item
      where  location_no = g_location_no and
             item_no     = g_rec_out.item_no;

      exception
        when no_data_found then
           g_rec_out.supply_chain_type := null;
        when others then
           l_message := 'Item Supp Country lookup failure '||sqlcode||' '||sqlerrm;
           dwh_log.record_error(l_module_name,sqlcode,l_message);
           raise;
   end;
*/
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into dim_item values a_tbl_insert(i);
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
      update dim_item
      set    diff_1_code_desc                = a_tbl_update(i).diff_1_code_desc,
             diff_2_code_desc                = a_tbl_update(i).diff_2_code_desc,
             diff_3_code_desc                = a_tbl_update(i).diff_3_code_desc,
             diff_4_code_desc                = a_tbl_update(i).diff_4_code_desc,
             diff_1_diff_type                = a_tbl_update(i).diff_1_diff_type,
             diff_2_diff_type                = a_tbl_update(i).diff_2_diff_type,
             diff_3_diff_type                = a_tbl_update(i).diff_3_diff_type,
             diff_4_diff_type                = a_tbl_update(i).diff_4_diff_type,
             diff_1_type_desc                = a_tbl_update(i).diff_1_type_desc,
             diff_2_type_desc                = a_tbl_update(i).diff_2_type_desc,
             diff_3_type_desc                = a_tbl_update(i).diff_3_type_desc,
             diff_4_type_desc                = a_tbl_update(i).diff_4_type_desc,
             diff_type_colour_diff_code      = a_tbl_update(i).diff_type_colour_diff_code,
             diff_type_prim_size_diff_code   = a_tbl_update(i).diff_type_prim_size_diff_code,
             diff_type_scnd_size_diff_code   = a_tbl_update(i).diff_type_scnd_size_diff_code,
             diff_type_fragrance_diff_code   = a_tbl_update(i).diff_type_fragrance_diff_code,
             diff_1_diff_group_code          = a_tbl_update(i).diff_1_diff_group_code,
             diff_2_diff_group_code          = a_tbl_update(i).diff_2_diff_group_code,
             diff_3_diff_group_code          = a_tbl_update(i).diff_3_diff_group_code,
             diff_4_diff_group_code          = a_tbl_update(i).diff_4_diff_group_code,
             diff_1_diff_group_desc          = a_tbl_update(i).diff_1_diff_group_desc,
             diff_2_diff_group_desc          = a_tbl_update(i).diff_2_diff_group_desc,
             diff_3_diff_group_desc          = a_tbl_update(i).diff_3_diff_group_desc,
             diff_4_diff_group_desc          = a_tbl_update(i).diff_4_diff_group_desc,
             diff_1_display_seq              = a_tbl_update(i).diff_1_display_seq,
             diff_2_display_seq              = a_tbl_update(i).diff_2_display_seq,
             diff_3_display_seq              = a_tbl_update(i).diff_3_display_seq,
             diff_4_display_seq              = a_tbl_update(i).diff_4_display_seq,
--             primary_supplier_no             = a_tbl_update(i).primary_supplier_no,
--             sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no,
--             supply_chain_type               = a_tbl_update(i).supply_chain_type,
             last_updated_date               = a_tbl_update(i).last_updated_date,
             item_level1_desc                = a_tbl_update(i).item_level1_desc,
             item_level1_long_desc           = a_tbl_update(i).item_level1_long_desc
      where  item_no                         = a_tbl_update(i).item_no  ;

      g_recs_updated := g_recs_updated + a_tbl_update.count;


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
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := TRUE;

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_item_no  := merch_hierachy_seq.nextval;
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
-- Main process loop
--**************************************************************************************************
begin

    dbms_output.put_line('Creating data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF DIM_ITEM EX DIM_ITEM AND DIFFS STARTED AT '||
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
    open c_dim_item;
    fetch c_dim_item bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_dim_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_item;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
end wh_prf_corp_036b;
