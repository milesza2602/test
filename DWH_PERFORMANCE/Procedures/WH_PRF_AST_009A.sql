--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_009A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_009A" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2012
--  Author:      Alfonso Joshua
--
-- Purpose:      Load Assort Style Colour MasterData in the performance layer
--               with data ex foundation layer tables.
--               This is the first step in 2 steps to load
--               data into the Assort Style Colour dimension table.
--
--  1st Step:    Will load place holder data from fnd_ast_lev1_diff1
--
--  2nd Step:    See a program called WH_PRF_AST_009U
--
--  Tables:      Input  -   fnd_ast_lev1_diff1
--               Output -   dim_ast_lev1_diff1
--
--  Packages:    constants, dwh_log,
--
--  Maintenance:
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor followed by table name
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

L_message            Sys_dwh_errlog.Log_text%Type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_009A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_bam;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ASSORT STYLE COLOUR DIMENSION EX FOUNDATION';
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
      select sk1_style_no, style_desc
        from dim_ast_lev1;
--Below Cursor is used on the company_descr_update procedure.
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
--Below Cursor is used on the style_subclass_descr_update procedure.
   cursor style_subclass_cur is
      with lista as (
        select sk1_style_no, count(distinct subclass_no)
        from   dim_ast_lev1_diff1
        where  business_unit_no <> 50
        group by sk1_style_no
        having count(distinct subclass_no) > 1),

      listb as (
        select dim.sk1_style_no, dim.sk1_subclass_no, dim.subclass_no, dim.subclass_long_desc, dim.last_updated_date
        from dim_ast_lev1_diff1 dim, lista l
        where dim.sk1_style_no = l.sk1_style_no
        group by dim.sk1_style_no, dim.sk1_subclass_no, dim.subclass_no, dim.subclass_long_desc , dim.last_updated_date
        order by dim.last_updated_date desc)

        select a.sk1_style_no, a.sk1_subclass_no, a.subclass_no, a.subclass_long_desc
        from   listb a
        where  a.last_updated_date = (select max(b.last_updated_date) from dim_ast_lev1_diff1 b
                                      where a.sk1_style_no = b.sk1_style_no);
--Below Cursor is used on the style_subclass_descr_update procedure.
   cursor subclass_class_cur is
      with lista as (
        select  sk1_subclass_no, count(distinct class_no)
        from dim_ast_lev1_diff1
        where business_unit_no <> 50
        group by sk1_subclass_no
        having count(distinct class_no) > 1),

      listb as (
        select dim.sk1_subclass_no, dim.sk1_class_no, dim.class_no, dim.class_long_desc, dim.last_updated_date
        from dim_ast_lev1_diff1 dim, lista l
        where dim.sk1_subclass_no = l.sk1_subclass_no
        group by dim.sk1_subclass_no, dim.sk1_class_no, dim.class_no, dim.class_long_desc, dim.last_updated_date
        order by dim.last_updated_date desc)

        select a.sk1_subclass_no, a.sk1_class_no, a.class_no, a.class_long_desc
        from listb a
        where a.last_updated_date = (select max(b.last_updated_date) from dim_ast_lev1_diff1 b
                                     where a.sk1_subclass_no = b.sk1_subclass_no);
--Below Cursor is used on the class_dept_descr_update procedure.
   cursor class_dept_cur is
      with lista as (
        select  sk1_class_no, count(distinct department_no)
        from dim_ast_lev1_diff1
        where business_unit_no <> 50
        group by sk1_class_no
        having count(distinct department_no) > 1),

      listb as (
        select dim.sk1_class_no, dim.sk1_department_no, dim.department_no, dim.department_long_desc, dim.last_updated_date
        from dim_ast_lev1_diff1 dim, lista l
        where dim.sk1_class_no = l.sk1_class_no
        group by dim.sk1_class_no, dim.sk1_department_no, dim.department_no, dim.department_long_desc, dim.last_updated_date
        order by dim.last_updated_date desc)

        select a.sk1_class_no, a.sk1_department_no, a.department_no, a.department_long_desc
        from listb a
        where a.last_updated_date = (select max(b.last_updated_date) from dim_ast_lev1_diff1 b
                                     where a.sk1_class_no = b.sk1_class_no);
--Below Cursor is used on the dept_subgroup_descr_update procedure.
   cursor dept_subgroup_cur is
with lista as (
        select  sk1_department_no, count(distinct subgroup_no)
        from dim_ast_lev1_diff1
        where business_unit_no <> 50
        group by sk1_department_no
        having count(distinct subgroup_no) > 1),

      listb as (
        select dim.sk1_department_no, dim.sk1_subgroup_no, dim.subgroup_no, dim.subgroup_long_desc, dim.last_updated_date
        from dim_ast_lev1_diff1 dim, lista l
        where dim.sk1_department_no = l.sk1_department_no
        group by dim.sk1_department_no, dim.sk1_subgroup_no, dim.subgroup_no, dim.subgroup_long_desc, dim.last_updated_date
        order by dim.last_updated_date desc)

        select a.sk1_department_no, a.sk1_subgroup_no, a.subgroup_no, a.subgroup_long_desc
        from listb a
        where a.last_updated_date = (select max(b.last_updated_date) from dim_ast_lev1_diff1 b
                                     where a.sk1_department_no = b.sk1_department_no);
--Below Cursor is used on the subgroup_group_descr_update procedure.
   cursor subgroup_group_cur is
with lista as (
        select  sk1_subgroup_no, count(distinct group_no)
        from dim_ast_lev1_diff1
        where business_unit_no <> 50
        group by sk1_subgroup_no
        having count(distinct group_no) > 1),

      listb as (
        select dim.sk1_subgroup_no, dim.sk1_group_no, dim.group_no, dim.group_long_desc, dim.last_updated_date
        from dim_ast_lev1_diff1 dim, lista l
        where dim.sk1_subgroup_no = l.sk1_subgroup_no
        group by dim.sk1_subgroup_no, dim.sk1_group_no, dim.group_no, dim.group_long_desc, dim.last_updated_date
        order by dim.last_updated_date desc)

        select a.sk1_subgroup_no, a.sk1_group_no, a.group_no, a.group_long_desc
        from listb a
        where a.last_updated_date = (select max(b.last_updated_date) from dim_ast_lev1_diff1 b
                                     where a.sk1_subgroup_no = b.sk1_subgroup_no);
--Below Cursor is used on the group_busunit_descr_update procedure.
   cursor group_busunit_cur is
with lista as (
        select  sk1_group_no, count(distinct business_unit_no)
        from dim_ast_lev1_diff1
        where business_unit_no <> 50
        group by sk1_group_no
        having count(distinct business_unit_no) > 1),

      listb as (
        select dim.sk1_group_no, dim.sk1_business_unit_no, dim.business_unit_no, dim.business_unit_long_desc, dim.last_updated_date
        from dim_ast_lev1_diff1 dim, lista l
        where dim.sk1_group_no = l.sk1_group_no
        group by dim.sk1_group_no, dim.sk1_business_unit_no, dim.business_unit_no, dim.business_unit_long_desc, dim.last_updated_date
        order by dim.last_updated_date desc)

        select a.sk1_group_no, a.sk1_business_unit_no, a.business_unit_no, a.business_unit_long_desc
        from listb a
        where a.last_updated_date = (select max(b.last_updated_date) from dim_ast_lev1_diff1 b
                                     where a.sk1_group_no = b.sk1_group_no);
--Below Cursor is used on the busunit_company_descr_update procedure.
   cursor busunit_company_cur is
with lista as (
        select  sk1_business_unit_no, count(distinct company_no)
        from dim_ast_lev1_diff1
        where business_unit_no <> 50
        group by sk1_business_unit_no
        having count(distinct company_no) > 1),

      listb as (
        select dim.sk1_business_unit_no, dim.sk1_company_no, dim.company_no, dim.company_long_desc, dim.last_updated_date
        from dim_ast_lev1_diff1 dim, lista l
        where dim.sk1_business_unit_no = l.sk1_business_unit_no
        group by dim.sk1_business_unit_no, dim.sk1_company_no, dim.company_no, dim.company_long_desc, dim.last_updated_date
        order by dim.last_updated_date desc)

        select a.sk1_business_unit_no, a.sk1_company_no, a.company_no, a.company_long_desc
        from listb a
        where a.last_updated_date = (select max(b.last_updated_date) from dim_ast_lev1_diff1 b
                                     where a.sk1_business_unit_no = b.sk1_business_unit_no);
--
  Cursor C_fnd_ast_lev1_diff1 Is

    select  ast.style_colour_no,
            ast.style_no,
            ast.subclass_no,
            ast.class_no,
            ast.department_no,
            ast.subgroup_no,
            ast.group_no,
            ast.business_unit_no,
            ast.company_no,
            ast.merch_season_no,
            nvl(ast.primary_supplier_no,0) primary_supplier_no,
            ast.purchase_type_no,
            ast.style_colour_desc,
            dbl.style_desc,
            ast.diff_1_code,
            ast.time_on_offer,
            ast.into_store_fin_week_code,
            nvl(ast.rpl_ind,0) rpl_ind,
            ast.origin_country_code,
            ast.supply_chain_code,
            subcl.sk1_subclass_no,
            subcl.sk1_class_no,
            dept.sk1_department_no,
            subg.sk1_subgroup_no,
            grp.sk1_group_no,
            unit.sk1_business_unit_no,
            comp.sk1_company_no,
            sup.sk1_supplier_no,
            subcl.subclass_name,
            subcl.class_name,
            dept.department_name,
            subg.subgroup_name,
            grp.group_name,
            unit.business_unit_name,
            comp.company_name,
            msp.sk1_merch_season_phase_no,
            rng.sk1_diff_range_no,
            Rng.Diff_range_no Diff_range_no,
            dbl.sk1_style_no,
            cal.this_week_start_date
    from    fnd_ast_lev1_diff1 ast,
            dim_merch_season_phase  msp,
            dim_subclass subcl,
            dim_class clas,
            dim_department dept,
            dim_subgroup subg,
            dim_group grp,
            dim_business_unit unit,
            dim_company comp,
            dim_supplier sup,
            Dim_diff_range Rng,
            dim_ast_lev1 Dbl,
            dim_calendar cal
     where  ast.last_updated_date                    = g_date
        and ast.Subclass_no                          = subcl.Subclass_no
        And ast.Class_no                             = subcl.Class_no
        and ast.department_no                        = subcl.department_no
        and ast.class_no                             = clas.class_no
        and ast.business_unit_no                     = clas.business_unit_no
        and ast.department_no                        = clas.department_no
        and ast.department_no                        = dept.department_no
        and ast.subgroup_no                          = subg.subgroup_no
        and ast.group_no                             = grp.group_no
        and ast.business_unit_no                     = unit.business_unit_no
        and ast.company_no                           = comp.company_no
        and nvl(ast.primary_supplier_no,0)           = sup.supplier_no
        and ast.merch_season_no                      = msp.merch_season_no
        and ast.diff_1_code                          = rng.diff_1_code
        And ast.Style_no                             = Dbl.Style_no
        and substr(ast.into_store_fin_week_code,2,4) = cal.fin_year_no
        and substr(ast.into_store_fin_week_code,6,2) = cal.fin_week_no
        and cal.fin_day_no                           = 3;

 g_rec_in     c_fnd_ast_lev1_diff1%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_ast_lev1_diff1%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
 begin

        G_rec_out.Style_colour_no               := G_rec_in.Style_colour_no;
        g_rec_out.sk1_style_no                  := g_rec_in.sk1_style_no;
        g_rec_out.style_colour_desc             := upper(g_rec_in.diff_1_code);
        g_rec_out.style_no                      := g_rec_in.style_no;
        g_rec_out.item_level1_no                := upper(g_rec_in.style_no);
        g_rec_out.subclass_no                   := g_rec_in.subclass_no;
        g_rec_out.class_no                      := g_rec_in.class_no;
        g_rec_out.department_no                 := g_rec_in.department_no;
        g_rec_out.subgroup_no                   := g_rec_in.subgroup_no;
        g_rec_out.group_no                      := g_rec_in.group_no;
        g_rec_out.business_unit_no              := g_rec_in.business_unit_no;
        g_rec_out.company_no                    := g_rec_in.company_no;
        g_rec_out.merch_season_no               := g_rec_in.merch_season_no;
        g_rec_out.primary_supplier_no           := g_rec_in.primary_supplier_no;
        g_rec_out.sk1_diff_1_range_no           := g_rec_in.sk1_diff_range_no;
        g_rec_out.diff_range_no                 := g_rec_in.diff_range_no;
        g_rec_out.diff_1_code                   := g_rec_in.diff_1_code;
        g_rec_out.rpl_ind                       := g_rec_in.rpl_ind;
        g_rec_out.into_store_start_date         := g_rec_in.this_week_start_date;
        g_rec_out.style_colour_type             := 'PLACEHOLDER';

        if g_rec_in.purchase_type_no is null then
           g_rec_out.purchase_type_no := 0;
        else
           g_rec_out.purchase_type_no           := g_rec_in.purchase_type_no;
        end if;

        if g_rec_in.supply_chain_code is null then
           g_rec_out.supply_chain_code := 'NSC';    --Defaulting the sk1_supply_chain_no to 0.
        else
           g_rec_out.supply_chain_code          := g_rec_in.supply_chain_code;
        end if;

        g_rec_out.origin_country_code           := g_rec_in.origin_country_code;
        g_rec_out.sk1_merch_season_phase_no     := g_rec_in.sk1_merch_season_phase_no;
        g_rec_out.sk1_subclass_no               := g_rec_in.sk1_subclass_no;
        g_rec_out.sk1_class_no                  := g_rec_in.sk1_class_no;
        g_rec_out.sk1_department_no             := g_rec_in.sk1_department_no;
        g_rec_out.sk1_subgroup_no               := g_rec_in.sk1_subgroup_no;
        g_rec_out.sk1_group_no                  := g_rec_in.sk1_group_no;
        g_rec_out.sk1_business_unit_no          := g_rec_in.sk1_business_unit_no;
        g_rec_out.sk1_company_no                := g_rec_in.sk1_company_no;
        g_rec_out.sk1_supplier_no               := g_rec_in.sk1_supplier_no;
        g_rec_out.diff_1_code_desc              := upper(g_rec_in.diff_1_code);
        g_rec_out.style_colour_long_desc        := upper(g_rec_in.style_colour_no||' - '||g_rec_in.style_desc||' - '||g_rec_in.diff_1_code);
        g_rec_out.style_long_desc               := upper(g_rec_in.style_no||' - '||g_rec_in.style_desc);

        g_rec_out.item_level1_long_desc         := upper(g_rec_in.style_no)||' - '||g_rec_in.style_desc;
        g_rec_out.item_level1_desc              := g_rec_in.style_desc;

        g_rec_out.subclass_long_desc            := upper(g_rec_in.subclass_no||' - '||g_rec_in.subclass_name);
        g_rec_out.class_long_desc               := upper(g_rec_in.class_no||' - '||g_rec_in.class_name);
        g_rec_out.department_long_desc          := upper(g_rec_in.department_no||' - '||g_rec_in.department_name);
        g_rec_out.subgroup_long_desc            := upper(g_rec_in.subgroup_no||' - '||g_rec_in.subgroup_name);
        g_rec_out.group_long_desc               := upper(g_rec_in.group_no||' - '||g_rec_in.group_name);
        g_rec_out.business_unit_long_desc       := upper(g_rec_in.business_unit_no||' - '||g_rec_in.business_unit_name);
        g_rec_out.company_long_desc             := upper(g_rec_in.company_no||' - '||g_rec_in.company_name);
        g_rec_out.last_updated_date             := g_date;
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
        g_rec_out.diff_1_diff_type              := null;
        g_rec_out.diff_1_type_desc              := null;
        g_rec_out.diff_1_diff_group_code        := null;
        g_rec_out.diff_1_diff_group_desc        := null;
        g_rec_out.diff_1_display_seq            := null;
        g_rec_out.diff_1_aggr_ind               := null;
        g_rec_out.total                         := null;
        g_rec_out.total_desc                    := null;
        g_rec_out.diff_type_colour_diff_code    := null;
        g_rec_out.diff_type_prim_size_diff_code := null;
        g_rec_out.diff_type_scnd_size_diff_code := null;
        g_rec_out.diff_type_fragrance_diff_code := null;
        g_rec_out.rp_catlg_ind                  := null;
        g_rec_out.supply_chain_type             := g_rec_in.supply_chain_code;
        g_rec_out.most_recent_merch_season_no   := null;
        g_rec_out.most_recent_merch_phase_no    := null;
        g_rec_out.avg_base_rsp                  := null;

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
                       ' '||a_tbl_insert(g_error_index).style_colour_no;
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
            set
                style_colour_desc          = a_tbl_update(i).style_colour_desc,
                style_no                   = a_tbl_update(i).style_no,
                item_level1_no             = a_tbl_update(i).item_level1_no,
                item_level1_desc           = a_tbl_update(i).item_level1_desc,
                subclass_no                = a_tbl_update(i).subclass_no,
                class_no                   = a_tbl_update(i).class_no,
                department_no              = a_tbl_update(i).department_no,
                subgroup_no                = a_tbl_update(i).subgroup_no,
                group_no                   = a_tbl_update(i).group_no,
                business_unit_no           = a_tbl_update(i).business_unit_no,
                company_no                 = a_tbl_update(i).company_no,
                merch_season_no            = a_tbl_update(i).merch_season_no,
                primary_supplier_no        = a_tbl_update(i).primary_supplier_no,
                sk1_diff_1_range_no        = a_tbl_update(i).sk1_diff_1_range_no,
                diff_range_no              = a_tbl_update(i).diff_range_no,
                diff_1_code                = a_tbl_update(i).diff_1_code,
                rpl_ind                    = a_tbl_update(i).rpl_ind,
                purchase_type_no           = a_tbl_update(i).purchase_type_no,
                origin_country_code        = a_tbl_update(i).origin_country_code,
                supply_chain_code          = a_tbl_update(i).supply_chain_code,
                Sk1_merch_season_phase_no  = a_tbl_update(I).Sk1_merch_season_phase_no,
                into_store_start_date      = a_tbl_update(i).into_store_start_date,
                sk1_style_no               = a_tbl_update(i).sk1_style_no,
                sk1_subclass_no            = a_tbl_update(i).sk1_subclass_no,
                sk1_class_no               = a_tbl_update(i).sk1_class_no,
                sk1_department_no          = a_tbl_update(i).sk1_department_no,
                sk1_subgroup_no            = a_tbl_update(i).sk1_subgroup_no,
                sk1_group_no               = a_tbl_update(i).sk1_group_no,
                sk1_business_unit_no       = a_tbl_update(i).sk1_business_unit_no,
                sk1_company_no             = a_tbl_update(i).sk1_company_no,
                sk1_supplier_no            = a_tbl_update(i).sk1_supplier_no,
                diff_1_code_desc           = a_tbl_update(i).diff_1_code_desc,
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
                style_colour_type          = a_tbl_update(i).style_colour_type,
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

         where style_colour_no      = a_tbl_update(i).style_colour_no;

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
                       ' '||a_tbl_update(g_error_index).style_colour_no;
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

   g_structure_count := v_count;

   l_text := 'STYLE DESCRIPTIONS TO BE PROCESSED :- '||v_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for style_rec in style_cur
        loop
         update dim_ast_lev1_diff1
            set item_level1_desc = style_rec.style_desc,
                item_level1_long_desc = item_level1_no||' - '||style_rec.style_desc
          where sk1_style_no = style_rec.sk1_style_no
           and  item_level1_desc <> style_rec.style_desc;

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

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

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

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

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

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

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

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

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

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

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

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

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

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

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
--*************************************************************************************************
-- To overwrite all the Subclass descriptions in the table with the latest description
--*************************************************************************************************
procedure style_subclass_update as
 begin

--  select count(*) into v_count
--    from dim_ast_lev1_diff1;

   l_text := 'STYLE/SUBCLASS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for style_subclass_rec in style_subclass_cur
        loop
         update dim_ast_lev1_diff1
            set sk1_subclass_no    = style_subclass_rec.sk1_subclass_no,
                subclass_no        = style_subclass_rec.subclass_no,
                subclass_long_desc = style_subclass_rec.subclass_long_desc
         where  sk1_style_no       = style_subclass_rec.sk1_style_no ;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'STYLE/SUBCLASS DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end style_subclass_update;
--*************************************************************************************************
-- To overwrite all the Class descriptions in the table with the latest description
--*************************************************************************************************
procedure subclass_class_update as
 begin

   l_text := 'SUBCLASS/CLASS TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for subclass_class_rec in subclass_class_cur
        loop
         update dim_ast_lev1_diff1
            set sk1_class_no    = subclass_class_rec.sk1_class_no,
                class_no        = subclass_class_rec.class_no,
                class_long_desc = subclass_class_rec.class_long_desc
         where  sk1_subclass_no = subclass_class_rec.sk1_subclass_no ;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'SUBCLASS/CLASS DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end subclass_class_update;
--*************************************************************************************************
-- To overwrite all the Department descriptions in the table with the latest description
--*************************************************************************************************
procedure class_dept_update as
 begin

   l_text := 'CLASS/DEPT TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for class_dept_rec in class_dept_cur
        loop
         update dim_ast_lev1_diff1
            set sk1_department_no    = class_dept_rec.sk1_department_no,
                department_no        = class_dept_rec.department_no,
                department_long_desc = class_dept_rec.department_long_desc
         where  sk1_class_no         = class_dept_rec.sk1_class_no ;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'CLASS/DEPT DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end class_dept_update;
--*************************************************************************************************
-- To overwrite all the Subgroup descriptions in the table with the latest description
--*************************************************************************************************
procedure dept_subgroup_update as
 begin

   l_text := 'DEPT/SUBGROUP TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for dept_subgroup_rec in dept_subgroup_cur
        loop
         update dim_ast_lev1_diff1
            set sk1_subgroup_no    = dept_subgroup_rec.sk1_subgroup_no,
                subgroup_no        = dept_subgroup_rec.subgroup_no,
                subgroup_long_desc = dept_subgroup_rec.subgroup_long_desc
         where  sk1_department_no  = dept_subgroup_rec.sk1_department_no ;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'DEPT/SUBGROUP DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end dept_subgroup_update;
--*************************************************************************************************
-- To overwrite all the Group descriptions in the table with the latest description
--*************************************************************************************************
procedure subgroup_group_update as
 begin

   l_text := 'SUBGROUP/GROUP TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for subgroup_group_rec in subgroup_group_cur
        loop
         update dim_ast_lev1_diff1
            set sk1_group_no    = subgroup_group_rec.sk1_group_no,
                group_no        = subgroup_group_rec.group_no,
                group_long_desc = subgroup_group_rec.group_long_desc
         where  sk1_subgroup_no = subgroup_group_rec.sk1_subgroup_no ;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'SUBGROUP/GROUP DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end subgroup_group_update;
--*************************************************************************************************
-- To overwrite all the Business Unit descriptions in the table with the latest description
--*************************************************************************************************
procedure group_busunit_update as
 begin

   l_text := 'GROUP/BUSUNIT TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for group_busunit_rec in group_busunit_cur
        loop
         update dim_ast_lev1_diff1
            set sk1_business_unit_no    = group_busunit_rec.sk1_business_unit_no,
                business_unit_no        = group_busunit_rec.business_unit_no,
                business_unit_long_desc = group_busunit_rec.business_unit_long_desc
         where  sk1_group_no            = group_busunit_rec.sk1_group_no ;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'GROUP/BUSUNIT DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end group_busunit_update;
--*************************************************************************************************
-- To overwrite all the Company descriptions in the table with the latest description
--*************************************************************************************************
procedure busunit_company_update as
 begin

   l_text := 'BUSUNIT/COMPANY TO BE PROCESSED :- '||g_structure_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    v_count := 0;

     for busunit_company_rec in busunit_company_cur
        loop
         update dim_ast_lev1_diff1
            set sk1_company_no       = busunit_company_rec.sk1_company_no,
                company_no           = busunit_company_rec.company_no,
                company_long_desc    = busunit_company_rec.company_long_desc
         where  sk1_business_unit_no = busunit_company_rec.sk1_business_unit_no ;

          v_count := v_count + sql%rowcount;
        end loop;

   l_text := 'BUSUNIT/COMPANY DESCRIPTIONS UPDATED :- '||v_count;
   dwh_log.Write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;
 end busunit_company_update;
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
    where style_colour_no  = g_rec_out.style_colour_no;

-- Generate an SK1 number for the associated Natural number if count = 0.
-- Note: count will always be 0, the test is for code template design consistancy.
   if g_count = 0 then
      G_rec_out.Sk1_style_colour_no := Merch_hierachy_seq.Nextval;
--      g_rec_out.sk1_style_no        := merch_hierachy_seq.nextval;
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

    l_text := 'LOAD OF DIM_AST_LEV1_DIFF1 EX FOUNDATION STARTED AT '||
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
    open c_fnd_ast_lev1_diff1;
    fetch c_fnd_ast_lev1_diff1 bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_ast_lev1_diff1 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_ast_lev1_diff1;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    style_descr_update;
/*
    l_text := 'GATHER STATS on dwh_performance.dim_ast_lev1_diff1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'dim_ast_lev1_diff1', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS  - Completed';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
    company_descr_update;
    bus_unit_descr_update;
    subclass_descr_update;
    class_descr_update;
    dept_descr_update;
    subgroup_descr_update;
    group_descr_update;
--
    style_subclass_update;
    subclass_class_update;
    class_dept_update;
    dept_subgroup_update;
    subgroup_group_update;
    group_busunit_update;
    busunit_company_update;

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
       Raise;
end wh_prf_ast_009a;
