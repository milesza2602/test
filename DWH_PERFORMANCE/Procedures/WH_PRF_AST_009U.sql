--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_009U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_009U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2012
--  Author:      Alfonso Joshua
--
-- Purpose:     Load Assort Style Colour MasterData in the performance layer
--              with data ex performance layer RMS
--              This is the second step to load data into the Assort Style Colour Dimension Table.
--
--  1st Step:   Refer to a program called WH_PRF_BAM_009A
--
--  2nd Step:   Will load data from dim_lev1_diff1 (ex RMS)
--
--  Tables:     Input  -   dim_lev1_diff1
--              Output -   dim_ast_lev1_diff1
--
--  Packages:   constants, dwh_log,
--
--  Maintenance:
--
--  Naming conventions
--  g_, v_  -  Global variable
--  l_      -  Log table variable
--  a_      -  Array variable
--  v_      -  Local variable as found in packages
--  p_      -  Parameter
--  c_      -  Prefix to cursor followed by table name
--**************************************************************************************************

g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_structure_count    number        :=  0;
g_rec_out            dim_ast_lev1_diff1%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

v_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_009U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_bam;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ASSORT STYLE COLOUR DIMENSION EX RMS PERFORMANCE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_ast_lev1_diff1%rowtype index by binary_integer;
type tbl_array_u is table of dim_ast_lev1_diff1%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

--Below Cursor is used on the style_descr_update procedure.

  cursor style_cur is
      select sk1_style_no, item_level1_long_desc, item_level1_no, ITEM_LEVEL1_DESC
        from dim_lev1;

--Main Cursor.

  cursor company_cur is
      select sk1_company_no, company_no, company_long_desc
        from dim_company;
--Below Cursor is used on the bus_unit_descr_update procedure.
  cursor bus_unit_cur is
      select sk1_business_unit_no, business_unit_no, business_unit_long_desc
        from dim_business_unit;
--Below Cursor is used on the subclass_descr_update procedure.
  cursor subclass_cur is
      select sk1_subclass_no, subclass_no, subclass_long_desc
        from dim_subclass;
--Below Cursor is used on the class_descr_update procedure.
  cursor class_cur is
      select sk1_class_no, class_no, class_long_desc
        from dim_class;
--Below Cursor is used on the dept_descr_update procedure.
  cursor dept_cur is
      select sk1_department_no, department_no, department_long_desc
        from dim_department;
--Below Cursor is used on the subgroup_descr_update procedure.
  cursor subgroup_cur is
      select sk1_subgroup_no, subgroup_no, subgroup_long_desc
        from dim_subgroup;
--Below Cursor is used on the group_descr_update procedure.
  cursor group_cur is
      select sk1_group_no, group_no, group_long_desc
        from dim_group;

  cursor c_dim_lev1_diff1 is
      select rms_ld.*,
             msp.merch_season_no,
             dr.diff_range_no,
             nvl(rtl_sup.purchase_type_no,0) purchase_type_no,
             nvl(rtl_sup.derived_country_code,'TBC') derived_country_code
      from   dim_lev1_diff1          rms_ld,
             dim_merch_season_phase  msp,
             dim_diff_range          dr,
             rtl_sc_supplier         rtl_sup

      where  rms_ld.sk1_style_colour_no       = rtl_sup.sk1_style_colour_no(+)
       and   rms_ld.sk1_supplier_no           = rtl_sup.sk1_supplier_no(+)
       and   rms_ld.sk1_merch_season_phase_no = msp.sk1_merch_season_phase_no
       and   rms_ld.sk1_diff_1_range_no       = dr.sk1_diff_range_no;
--       and   rms_ld.last_updated_date         = g_date;

 g_rec_in     c_dim_lev1_diff1%rowtype;

-- For input bulk collect --
type stg_array is table of c_dim_lev1_diff1%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
 begin
        g_rec_out.sk1_style_colour_no       := g_rec_in.sk1_style_colour_no;
        g_rec_out.style_colour_no           := g_rec_in.style_colour_no;
        g_rec_out.style_colour_desc         := g_rec_in.item_level1_desc||' - '||g_rec_in.diff_1_code_desc;
        g_rec_out.sk1_style_no              := g_rec_in.sk1_style_no;
        g_rec_out.style_no                  := g_rec_in.style_no;
        g_rec_out.item_level1_no            := g_rec_in.item_level1_no;
        g_rec_out.item_level1_desc          := g_rec_in.item_level1_desc;
        g_rec_out.sk1_subclass_no           := g_rec_in.sk1_subclass_no;
        g_rec_out.subclass_no               := g_rec_in.subclass_no;
        g_rec_out.sk1_class_no              := g_rec_in.sk1_class_no;
        g_rec_out.class_no                  := g_rec_in.class_no;
        g_rec_out.sk1_department_no         := g_rec_in.sk1_department_no;
        g_rec_out.department_no             := g_rec_in.department_no;
        g_rec_out.sk1_subgroup_no           := g_rec_in.sk1_subgroup_no;
        g_rec_out.subgroup_no               := g_rec_in.subgroup_no;
        g_rec_out.sk1_group_no              := g_rec_in.sk1_group_no;
        g_rec_out.group_no                  := g_rec_in.group_no;
        g_rec_out.sk1_business_unit_no      := g_rec_in.sk1_business_unit_no;
        g_rec_out.business_unit_no          := g_rec_in.business_unit_no;
        g_rec_out.sk1_company_no            := g_rec_in.sk1_company_no;
        g_rec_out.company_no                := g_rec_in.company_no;
        g_rec_out.sk1_merch_season_phase_no := g_rec_in.sk1_merch_season_phase_no;
        g_rec_out.merch_season_no           := g_rec_in.merch_season_no;
        g_rec_out.sk1_supplier_no           := g_rec_in.sk1_supplier_no;
        g_rec_out.primary_supplier_no       := g_rec_in.primary_supplier_no;
        g_rec_out.sk1_diff_1_range_no       := g_rec_in.sk1_diff_1_range_no;
        g_rec_out.diff_range_no             := g_rec_in.diff_range_no;
        g_rec_out.diff_1_code               := g_rec_in.diff_1_code;
        g_rec_out.diff_1_code_desc          := g_rec_in.diff_1_code_desc;
        g_rec_out.rpl_ind                   := nvl(g_rec_in.rpl_ind,0);
        g_rec_out.purchase_type_no          := g_rec_in.purchase_type_no;
        g_rec_out.origin_country_code       := g_rec_in.derived_country_code;

        if g_rec_in.supply_chain_type is null then
           g_rec_out.supply_chain_code      := 'NSC';    --Defaulting the sk1_supply_chain_no to 0.
        else
           g_rec_out.supply_chain_code      := g_rec_in.supply_chain_type;
        end if;

        g_rec_out.style_colour_long_desc    := g_rec_in.style_colour_no||' - '||g_rec_in.item_level1_desc||' - '||g_rec_in.diff_1_code_desc;
        g_rec_out.item_level1_long_desc     := g_rec_in.item_level1_no||' - '||g_rec_in.item_level1_desc;
        g_rec_out.style_long_desc           := g_rec_in.item_level1_no||' - '||g_rec_in.item_level1_desc;
        g_rec_out.subclass_long_desc        := g_rec_in.subclass_no||' - '||g_rec_in.subclass_name;
        g_rec_out.class_long_desc           := g_rec_in.class_no||' - '||g_rec_in.class_name;
        g_rec_out.department_long_desc      := g_rec_in.department_no||' - '||g_rec_in.department_name;
        g_rec_out.subgroup_long_desc        := g_rec_in.subgroup_no||' - '||g_rec_in.subgroup_name;
        g_rec_out.group_long_desc           := g_rec_in.group_no||' - '||g_rec_in.group_name;
        g_rec_out.business_unit_long_desc   := g_rec_in.business_unit_no||' - '||g_rec_in.business_unit_name;
        g_rec_out.company_long_desc         := g_rec_in.company_no||' - '||g_rec_in.company_name;
        g_rec_out.last_updated_date         := g_date;
--
-- Added for JDA Phase II
--
        g_rec_out.subclass_name                 := g_rec_in.subclass_name;
        g_rec_out.class_name                    := g_rec_in.class_name;
        g_rec_out.department_name               := g_rec_in.department_name;
        g_rec_out.subgroup_name                 := g_rec_in.subgroup_name;
        g_rec_out.group_name                    := g_rec_in.group_name;
        g_rec_out.business_unit_name            := g_rec_in.business_unit_name;
        g_rec_out.company_name                  := g_rec_in.company_name;
        g_rec_out.diff_1_diff_type              := g_rec_in.diff_1_diff_type;
        g_rec_out.diff_1_type_desc              := g_rec_in.diff_1_type_desc;
        g_rec_out.diff_1_diff_group_code        := g_rec_in.diff_1_diff_group_code;
        g_rec_out.diff_1_diff_group_desc        := g_rec_in.diff_1_diff_group_desc;
        g_rec_out.diff_1_display_seq            := g_rec_in.diff_1_display_seq;
        g_rec_out.diff_1_aggr_ind               := g_rec_in.diff_1_aggr_ind;
        g_rec_out.total                         := g_rec_in.total;
        g_rec_out.total_desc                    := g_rec_in.total_desc;
        g_rec_out.diff_type_colour_diff_code    := g_rec_in.diff_type_colour_diff_code;
        g_rec_out.diff_type_prim_size_diff_code := g_rec_in.diff_type_prim_size_diff_code;
        g_rec_out.diff_type_scnd_size_diff_code := g_rec_in.diff_type_scnd_size_diff_code;
        g_rec_out.diff_type_fragrance_diff_code := g_rec_in.diff_type_fragrance_diff_code;
        g_rec_out.rp_catlg_ind                  := g_rec_in.rp_catlg_ind;
        g_rec_out.supply_chain_type             := g_rec_in.supply_chain_type;
        g_rec_out.most_recent_merch_season_no   := g_rec_in.most_recent_merch_season_no;
        g_rec_out.most_recent_merch_phase_no    := g_rec_in.most_recent_merch_phase_no;
        g_rec_out.avg_base_rsp                  := g_rec_in.avg_base_rsp;

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
       insert into dim_ast_lev1_diff1 values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no;
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
         update dim_ast_lev1_diff1
         set    style_colour_no            = a_tbl_update(i).style_colour_no,
                style_colour_desc          = a_tbl_update(i).style_colour_desc,
                sk1_style_no               = a_tbl_update(i).sk1_style_no,
                style_no                   = a_tbl_update(i).style_no,
                item_level1_no             = a_tbl_update(i).item_level1_no,
                item_level1_desc           = a_tbl_update(i).item_level1_desc,
                sk1_subclass_no            = a_tbl_update(i).sk1_subclass_no,
                subclass_no                = a_tbl_update(i).subclass_no,
                sk1_class_no               = a_tbl_update(i).sk1_class_no,
                class_no                   = a_tbl_update(i).class_no,
                sk1_department_no          = a_tbl_update(i).sk1_department_no,
                department_no              = a_tbl_update(i).department_no,
                sk1_subgroup_no            = a_tbl_update(i).sk1_subgroup_no,
                subgroup_no                = a_tbl_update(i).subgroup_no,
                sk1_group_no               = a_tbl_update(i).sk1_group_no,
                group_no                   = a_tbl_update(i).group_no,
                sk1_business_unit_no       = a_tbl_update(i).sk1_business_unit_no,
                business_unit_no           = a_tbl_update(i).business_unit_no,
                sk1_company_no             = a_tbl_update(i).sk1_company_no,
                company_no                 = a_tbl_update(i).company_no,
                sk1_merch_season_phase_no  = a_tbl_update(i).sk1_merch_season_phase_no,
                merch_season_no            = a_tbl_update(i).merch_season_no,
                sk1_supplier_no            = a_tbl_update(i).sk1_supplier_no,
                primary_supplier_no        = a_tbl_update(i).primary_supplier_no,
                sk1_diff_1_range_no        = a_tbl_update(i).sk1_diff_1_range_no,
                diff_range_no              = a_tbl_update(i).diff_range_no,
                diff_1_code                = a_tbl_update(i).diff_1_code,
                diff_1_code_desc           = a_tbl_update(i).diff_1_code_desc,
                rpl_ind                    = a_tbl_update(i).rpl_ind,
                purchase_type_no           = a_tbl_update(i).purchase_type_no,
                origin_country_code        = a_tbl_update(i).origin_country_code,
                supply_chain_code          = a_tbl_update(i).supply_chain_code,
                style_colour_long_desc     = a_tbl_update(i).style_colour_long_desc,
                item_level1_long_desc      = a_tbl_update(i).item_level1_long_desc,
                style_long_desc            = a_tbl_update(i).style_long_desc,
                subclass_long_desc         = a_tbl_update(i).subclass_long_desc,
                class_long_desc            = a_tbl_update(i).class_long_desc,
                department_long_desc       = a_tbl_update(i).department_long_desc,
                subgroup_long_desc         = a_tbl_update(i).subgroup_long_desc,
                group_long_desc            = a_tbl_update(i).group_long_desc,
                business_unit_long_desc    = a_tbl_update(i).business_unit_long_desc,
                company_long_desc          = a_tbl_update(i).company_long_desc,
                last_updated_date          = a_tbl_update(i).last_updated_date,
--
-- Added for JDA Assort Phase II
--
                subclass_name                 = a_tbl_update(i).subclass_name,
                class_name                    = a_tbl_update(i).class_name,
                department_name               = a_tbl_update(i).department_name,
                subgroup_name                 = a_tbl_update(i).subgroup_name,
                group_name                    = a_tbl_update(i).group_name,
                business_unit_name            = a_tbl_update(i).business_unit_name,
                company_name                  = a_tbl_update(i).company_name,
                diff_1_diff_type              = a_tbl_update(i).diff_1_diff_type,
                diff_1_type_desc              = a_tbl_update(i).diff_1_type_desc,
                diff_1_diff_group_code        = a_tbl_update(i).diff_1_diff_group_code,
                diff_1_diff_group_desc        = a_tbl_update(i).diff_1_diff_group_desc,
                diff_1_display_seq            = a_tbl_update(i).diff_1_display_seq,
                diff_1_aggr_ind               = a_tbl_update(i).diff_1_aggr_ind,
                total                         = a_tbl_update(i).total,
                total_desc                    = a_tbl_update(i).total_desc,
                diff_type_colour_diff_code    = a_tbl_update(i).diff_type_colour_diff_code,
                diff_type_prim_size_diff_code = a_tbl_update(i).diff_type_prim_size_diff_code,
                diff_type_scnd_size_diff_code = a_tbl_update(i).diff_type_scnd_size_diff_code,
                diff_type_fragrance_diff_code = a_tbl_update(i).diff_type_fragrance_diff_code,
                rp_catlg_ind                  = a_tbl_update(i).rp_catlg_ind,
                supply_chain_type             = a_tbl_update(i).supply_chain_type,
                most_recent_merch_season_no   = a_tbl_update(i).most_recent_merch_season_no,
                most_recent_merch_phase_no    = a_tbl_update(i).most_recent_merch_phase_no,
                avg_base_rsp                  = a_tbl_update(i).avg_base_rsp
         where sk1_style_colour_no            = a_tbl_update(i).sk1_style_colour_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
      raise;
end local_bulk_update;

--*************************************************************************************************
-- To overwrite all the Style_Descriptions in the table with the latest description
--*************************************************************************************************
procedure style_descr_update as
 begin

  select count(*) into v_count
    from dim_ast_lev1_diff1;

   l_text := 'STYLE DESCRIPTIONS TO BE PROCESSED :- '||v_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for style_rec in style_cur
        loop
         update dim_ast_lev1_diff1
            set item_level1_no        = style_rec.item_level1_no,
                item_level1_desc      = style_rec.item_level1_desc,
                item_level1_long_desc = style_rec.item_level1_long_desc
          where sk1_style_no          = style_rec.sk1_style_no
           and  item_level1_long_desc     <> style_rec.item_level1_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'STYLE DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end style_descr_update;

--*************************************************************************************************
-- To overwrite all the Subclass descriptions in the table with the latest description
--*************************************************************************************************
procedure company_descr_update as
 begin

   select count(*) into v_count
   from dim_ast_lev1_diff1;

   g_structure_count := v_count;

   l_text := 'COMPANY DESCRIPTIONS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for company_rec in company_cur
        loop
         update dim_ast_lev1_diff1
            set company_no        = company_rec.company_no,
                company_long_desc = company_rec.company_long_desc
         where  sk1_company_no    = company_rec.sk1_company_no
          and   company_long_desc <> company_rec.company_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'COMPANY DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end company_descr_update;
--*************************************************************************************************
-- To overwrite all the Subclass descriptions in the table with the latest description
--*************************************************************************************************
procedure bus_unit_descr_update as
 begin

   l_text := 'BUSINESS UNIT DESCRIPTIONS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for bus_unit_rec in bus_unit_cur
        loop
         update dim_ast_lev1_diff1
            set business_unit_no        = bus_unit_rec.business_unit_no,
                business_unit_long_desc = bus_unit_rec.business_unit_long_desc
         where  sk1_business_unit_no    = bus_unit_rec.sk1_business_unit_no
          and   business_unit_long_desc <> bus_unit_rec.business_unit_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'BUSINESS UNIT DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end bus_unit_descr_update;
--*************************************************************************************************
-- To overwrite all the Subclass descriptions in the table with the latest description
--*************************************************************************************************
procedure subclass_descr_update as
 begin

   l_text := 'SUBCLASS DESCRIPTIONS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for subclass_rec in subclass_cur
        loop
         update dim_ast_lev1_diff1
            set subclass_no        = subclass_rec.subclass_no,
                subclass_long_desc = subclass_rec.subclass_long_desc
         where  sk1_subclass_no    = subclass_rec.sk1_subclass_no
          and   subclass_long_desc <> subclass_rec.subclass_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'SUBCLASS DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end subclass_descr_update;
--*************************************************************************************************
-- To overwrite all the Class descriptions in the table with the latest description
--*************************************************************************************************
procedure class_descr_update as
 begin

   l_text := 'CLASS DESCRIPTIONS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for class_rec in class_cur
        loop
         update dim_ast_lev1_diff1
            set class_no        = class_rec.class_no,
                class_long_desc = class_rec.class_long_desc
         where  sk1_class_no    = class_rec.sk1_class_no
           and  class_long_desc <> class_rec.class_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'CLASS DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end class_descr_update;
--*************************************************************************************************
-- To overwrite all the Department descriptions in the table with the latest description
--*************************************************************************************************
procedure dept_descr_update as
 begin

   l_text := 'DEPARTMENT DESCRIPTIONS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for dept_rec in dept_cur
        loop
         update dim_ast_lev1_diff1
            set department_no        = dept_rec.department_no,
                department_long_desc = dept_rec.department_long_desc
         where  sk1_department_no    = dept_rec.sk1_department_no
          and   department_long_desc <> dept_rec.department_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'DEPARTMENT DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end dept_descr_update;
--*************************************************************************************************
-- To overwrite all the Subgroup descriptions in the table with the latest description
--*************************************************************************************************
procedure subgroup_descr_update as
 begin

   l_text := 'SUBGROUP DESCRIPTIONS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for subgroup_rec in subgroup_cur
        loop
         update dim_ast_lev1_diff1
            set subgroup_no        = subgroup_rec.subgroup_no,
                subgroup_long_desc = subgroup_rec.subgroup_long_desc
         where  sk1_subgroup_no    = subgroup_rec.sk1_subgroup_no
          and   subgroup_long_desc <> subgroup_rec.subgroup_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'SUBGROUP DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end subgroup_descr_update;
--*************************************************************************************************
-- To overwrite all the Group descriptions in the table with the latest description
--*************************************************************************************************
procedure group_descr_update as
 begin

   l_text := 'GROUP DESCRIPTIONS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for group_rec in group_cur
        loop
         update dim_ast_lev1_diff1
            set group_no        = group_rec.group_no,
                group_long_desc = group_rec.group_long_desc
         where  sk1_group_no    = group_rec.sk1_group_no
          and   group_long_desc <> group_rec.group_long_desc;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'GROUP DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end group_descr_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
 begin

   g_found := FALSE;
   g_count :=0;

-- Check to see if the style_colour_no is present on table and update/insert accordingly.
   select count(1)
     into g_count
     from dim_ast_lev1_diff1
    where sk1_style_colour_no  = g_rec_out.sk1_style_colour_no;

-- Place record into array for later bulk writing
-- Note: Count will always be 0 thus insert will be executed, always no update.
   if g_count = 0 then
      a_count_i                     := a_count_i + 1;
      a_tbl_insert(a_count_i)       := g_rec_out;

    else
      a_count_u                   := a_count_u + 1;
      a_tbl_update(a_count_u)     := g_rec_out;

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

    l_text := 'LOAD OF DIM_AST_LEV1_DIFF1 EX PERFORMANCE (ex RMS) STARTED AT '||
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
    open c_dim_lev1_diff1;
    fetch c_dim_lev1_diff1 bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 50000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_dim_lev1_diff1 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_lev1_diff1;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    style_descr_update;
--**************************************************************************************************
-- Temporarily measure in place to ensure that only one hierarchy level description is stored on masterdata
--**************************************************************************************************
    company_descr_update;
    bus_unit_descr_update;
    subclass_descr_update;
    class_descr_update;
    dept_descr_update;
    subgroup_descr_update;
    group_descr_update;

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
end WH_PRF_AST_009U;
