--------------------------------------------------------
--  DDL for Procedure WH_FND_AST_009U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_AST_009U" (p_forall_limit in integer,p_success out boolean) as

-- **************************************************************************************************
--  Date:        25 April 2012
--  Author:      Alfonso Joshua
--  Purpose:     Create a dim_lev1_diff1 ASSORT interface
--               on the foundation layer
--
--  Tables:      Input  - stg_ast_lev1_diff1_cpy
--               Output - fnd_ast_lev1_diff1
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_ast_lev1_diff1_hsp.sys_process_msg%type;
g_rec_out            fnd_ast_lev1_diff1%rowtype;
g_rec_in             stg_ast_lev1_diff1_cpy%rowtype;

g_found              boolean;
g_insert_rec         boolean;
g_invalid_plan_type_no boolean;
g_date               date          := trunc(sysdate);

g_restructure_ind    dim_control.restructure_ind%type;
g_company_no         fnd_company.company_no%type;
g_business_unit_no   fnd_business_unit.business_unit_no%type;
g_group_no           fnd_group.group_no%type;
g_subgroup_no        fnd_subgroup.subgroup_no%type;
g_department_no      fnd_department.department_no%type;
g_class_no           fnd_class.class_no%type;
g_subclass_no        fnd_subclass.subclass_no%type;
g_style_colour_no    fnd_ast_lev1_diff1.style_colour_no%type;
g_style_no           fnd_ast_lev1_diff1.style_no%type;
g_bypass_restruct_chk char(1)       := 'N';

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_AST_009U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_pln_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ASSORT LEV1 DIFF1 (SC) MASTER DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For input bulk collect --
type stg_array is table of stg_ast_lev1_diff1_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_ast_lev1_diff1%rowtype index by binary_integer;
type tbl_array_u is table of fnd_ast_lev1_diff1%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_ast_lev1_diff1_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_ast_lev1_diff1_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_bam_lev1_diff1 is
     select   *
     from     stg_ast_lev1_diff1_cpy
     where    sys_process_code = 'N'
     order by sys_source_batch_id, sys_source_sequence_no;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure check_restructure as
begin

   g_bypass_restruct_chk := 'N';

   if g_restructure_ind = 0 then
-- company ----> business unit
     begin
        select company_no
        into   g_company_no
        from   fnd_business_unit
        where  business_unit_no = g_rec_out.business_unit_no;

        exception
        when no_data_found then
           g_company_no := g_rec_out.company_no;
      end;

      if g_company_no <> g_rec_out.company_no then
         g_bypass_restruct_chk := 'Y';
         dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                  'fnd_business_unit',g_rec_out.business_unit_no,g_company_no,g_rec_out.company_no);
         g_hospital      := 'Y';
         g_hospital_text := 'Trying to illegally restructure hierarchy - business_unit ';
         l_text          := 'Trying to illegally restructure hierarchy - business_unit'||g_rec_out.business_unit_no||' '||g_rec_out.company_no;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

-- business unit ----> group
      if g_bypass_restruct_chk = 'N' then
         begin
           select business_unit_no
           into   g_business_unit_no
           from   fnd_group
           where  group_no = g_rec_out.group_no;

           exception
              when no_data_found then
                 g_business_unit_no := g_rec_out.business_unit_no;
         end;

         if g_business_unit_no <> g_rec_out.business_unit_no then
            g_bypass_restruct_chk := 'Y';
            dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                     'fnd_group',g_rec_out.group_no,g_business_unit_no,g_rec_out.business_unit_no);
            g_hospital      := 'Y';
            g_hospital_text := 'Trying to illegally restructure hierarchy - group_no ';
            l_text          := 'Trying to illegally restructure hierarchy - group_no'||g_rec_out.group_no||' '||g_rec_out.business_unit_no;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;
      end if;

-- group ----> subgroup
      if g_bypass_restruct_chk = 'N' then
         begin
           select group_no
           into   g_group_no
           from   fnd_subgroup
           where  subgroup_no = g_rec_out.subgroup_no;

           exception
              when no_data_found then
                 g_group_no := g_rec_out.group_no;
         end;

         if g_group_no <> g_rec_out.group_no then
            g_bypass_restruct_chk := 'Y';
            dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                     'fnd_subgroup',g_rec_out.subgroup_no,g_group_no,g_rec_out.group_no);
            g_hospital      := 'Y';
            g_hospital_text := 'Trying to illegally restructure hierarchy - subgroup_no ';
            l_text          := 'Trying to illegally restructure hierarchy - subgroup_no'||g_rec_out.subgroup_no||' '||g_rec_out.group_no;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;
      end if;
-- subgroup ----> department
      if g_bypass_restruct_chk = 'N' then
         begin
           select subgroup_no
           into   g_subgroup_no
           from   fnd_department
           where  department_no = g_rec_out.department_no;

           exception
              when no_data_found then
                 g_subgroup_no := g_rec_out.subgroup_no;
         end;

         if g_subgroup_no <> g_rec_out.subgroup_no then
            g_bypass_restruct_chk := 'Y';
            dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                     'fnd_department',g_rec_out.department_no,g_subgroup_no,g_rec_out.subgroup_no);
            g_hospital      := 'Y';
            g_hospital_text := 'Trying to illegally restructure hierarchy - department_no ';
            l_text          := 'Trying to illegally restructure hierarchy - department_no '||g_rec_out.department_no||' '||g_rec_out.subgroup_no;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;
      end if;
-- product hierarchy check
      if g_bypass_restruct_chk = 'N' then
         g_style_colour_no := g_rec_out.style_colour_no;
         begin
           select style_no, subclass_no, class_no, department_no, subgroup_no, group_no, business_unit_no, company_no
           into  g_style_no, g_subclass_no, g_class_no, g_department_no, g_subgroup_no, g_group_no, g_business_unit_no, g_company_no
           from dim_ast_lev1_diff1
           where style_colour_no = g_rec_out.style_colour_no;

         exception
              when no_data_found then
--                 g_style_colour_no := g_rec_out.style_colour_no;
                 g_style_colour_no := 0;
         end;

        if g_style_colour_no <> 0 then
         if g_subclass_no <> g_rec_out.subclass_no then
            g_bypass_restruct_chk := 'Y';
            dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                     'dim_ast_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.subclass_no,g_subclass_no);
            g_hospital      := 'Y';
            g_hospital_text := 'Trying to illegally restructure hierarchy ';
            l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.subclass_no;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         if g_bypass_restruct_chk = 'N' then
            if g_style_no <> g_rec_out.style_no then
               g_bypass_restruct_chk := 'Y';
               dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
--                                        'dim_bam_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.style_no,0);
                                        'dim_ast_lev1_diff1',g_rec_out.style_colour_no,0,0);
               g_hospital      := 'Y';
               g_hospital_text := 'Trying to illegally restructure hierarchy ';
--               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.style_no;
               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||0;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;

         if g_bypass_restruct_chk = 'N' then
            if g_class_no <> g_rec_out.class_no then
               g_bypass_restruct_chk := 'Y';
               dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                        'dim_bam_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.class_no,g_class_no);
               g_hospital      := 'Y';
               g_hospital_text := 'Trying to illegally restructure hierarchy ';
               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.class_no;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;

         if g_bypass_restruct_chk = 'N' then
            if g_department_no <> g_rec_out.department_no then
               g_bypass_restruct_chk := 'Y';
               dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                        'dim_bam_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.department_no,g_department_no);
               g_hospital      := 'Y';
               g_hospital_text := 'Trying to illegally restructure hierarchy ';
               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.department_no;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;

         if g_bypass_restruct_chk = 'N' then
            if g_subgroup_no <> g_rec_out.subgroup_no then
               g_bypass_restruct_chk := 'Y';
               dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                        'dim_ast_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.subgroup_no,g_subgroup_no);
               g_hospital      := 'Y';
               g_hospital_text := 'Trying to illegally restructure hierarchy ';
               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.subgroup_no;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;

         if g_bypass_restruct_chk = 'N' then
            if g_group_no <> g_rec_out.group_no then
               g_bypass_restruct_chk := 'Y';
               dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                        'dim_ast_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.group_no,g_group_no);
               g_hospital      := 'Y';
               g_hospital_text := 'Trying to illegally restructure hierarchy ';
               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.group_no;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;

         if g_bypass_restruct_chk = 'N' then
            if g_business_unit_no <> g_rec_out.business_unit_no then
               g_bypass_restruct_chk := 'Y';
               dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                        'dim_ast_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.business_unit_no,g_business_unit_no);
               g_hospital      := 'Y';
               g_hospital_text := 'Trying to illegally restructure hierarchy ';
               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.business_unit_no;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;

         if g_bypass_restruct_chk = 'N' then
            if g_company_no <> g_rec_out.company_no then
               g_bypass_restruct_chk := 'Y';
               dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                        'dim_ast_lev1_diff1',g_rec_out.style_colour_no,g_rec_out.company_no,g_company_no);
               g_hospital      := 'Y';
               g_hospital_text := 'Trying to illegally restructure hierarchy ';
               l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.style_colour_no||' '||g_rec_out.company_no;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;
        end if;
      end if;
   end if;

end check_restructure;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as

 v_count              number              :=  0;

 begin
   g_hospital                              := 'N';

   g_rec_out.style_colour_no               := g_rec_in.style_colour_no;
   g_rec_out.style_no                      := g_rec_in.style_no;
   g_rec_out.subclass_no                   := g_rec_in.subclass_no;
   g_rec_out.class_no                      := g_rec_in.class_no;
   g_rec_out.department_no                 := g_rec_in.department_no;
   g_rec_out.subgroup_no                   := g_rec_in.subgroup_no;
   g_rec_out.group_no                      := g_rec_in.group_no;
   g_rec_out.business_unit_no              := g_rec_in.business_unit_no;
   g_rec_out.company_no                    := g_rec_in.company_no;
   g_rec_out.merch_season_no               := g_rec_in.merch_season_no;
   g_rec_out.primary_supplier_no           := g_rec_in.primary_supplier_no;
--   g_rec_out.diff_range_no                 := g_rec_in.diff_range_no;
   g_rec_out.purchase_type_no              := g_rec_in.purchase_type_no;
   g_rec_out.style_colour_desc             := g_rec_in.style_colour_desc;
   g_rec_out.style_desc                    := g_rec_in.style_desc;
   g_rec_out.diff_1_code                   := g_rec_in.diff_1_code;
   g_rec_out.into_store_fin_week_code      := g_rec_in.into_store_fin_week_code;
   g_rec_out.time_on_offer                 := g_rec_in.time_on_offer;
   g_rec_out.rpl_ind                       := g_rec_in.rpl_ind;
   g_rec_out.origin_country_code           := g_rec_in.origin_country_code;
   g_rec_out.supply_chain_code             := g_rec_in.supply_chain_type;
   g_rec_out.size_curve_no                 := g_rec_in.size_curve_no;
   g_rec_out.size_curve_desc               := g_rec_in.size_curve_desc;
   g_rec_out.last_updated_date             := g_date;

    if not dwh_valid.fnd_department(g_rec_out.department_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_dept_not_found;
     l_text := dwh_constants.vc_dept_not_found||' '||g_rec_out.department_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_subgroup(g_rec_out.subgroup_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_subgroup_not_found;
     l_text := dwh_constants.vc_subgroup_not_found||' '||g_rec_out.subgroup_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_group(g_rec_out.group_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_group_not_found;
     l_text := dwh_constants.vc_group_not_found||' '||g_rec_out.group_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_business_unit(g_rec_out.business_unit_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_bu_not_found;
     l_text := dwh_constants.vc_bu_not_found||' '||g_rec_out.business_unit_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_company(g_rec_out.company_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_company_not_found;
     l_text := dwh_constants.vc_company_not_found||' '||g_rec_out.company_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
/*
   if not dwh_valid.fnd_diff_range(g_rec_out.diff_range_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_diff_range_not_found;
     l_text := dwh_constants.vc_diff_range_not_found||' '||g_rec_out.diff_range_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
*/
   if not dwh_valid.fnd_merch_season(g_rec_out.merch_season_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_merch_season_not_found;
     l_text := dwh_constants.vc_merch_season_not_found||' '||g_rec_out.merch_season_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if g_rec_out.primary_supplier_no is not null then
      if not dwh_valid.fnd_supplier(g_rec_out.primary_supplier_no) then
        g_hospital      := 'Y';
        g_hospital_text := dwh_constants.vc_supplier_not_found;
        l_text := dwh_constants.vc_supplier_not_found||' '||g_rec_out.primary_supplier_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
   end if;

   if not dwh_valid.fnd_subclass(g_rec_out.subclass_no,g_rec_out.class_no,g_rec_out.department_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_subclass_not_found;
     l_text := dwh_constants.vc_subclass_not_found||' '||g_rec_out.subclass_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_class(g_rec_out.class_no,g_rec_out.department_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_class_not_found;
     l_text := dwh_constants.vc_class_not_found||' '||g_rec_out.class_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

-- check for any records that are set for restructure
   check_restructure;

   if (g_rec_out.purchase_type_no in (1,2,3) or
       g_rec_out.purchase_type_no is null) then null;
   else
       g_hospital      := 'Y';
       g_hospital_text := 'INVALID PURCHASE TYPE VALUES ';
       l_text          := 'INVALID PURCHASE TYPE VALUES '||g_rec_out.purchase_type_no;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if g_rec_out.supply_chain_code is not null then
      if not dwh_valid.fnd_supply_chain_code(g_rec_out.supply_chain_code) then
        g_hospital      := 'Y';
        g_hospital_text := dwh_constants.vc_supply_chain_not_found;
        l_text := dwh_constants.vc_supply_chain_not_found||' '||g_rec_out.supply_chain_code;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
   end if;

   if g_rec_out.origin_country_code is not null then
      if not dwh_valid.fnd_country(g_rec_out.origin_country_code) then
        g_hospital      := 'Y';
        g_hospital_text := dwh_constants.vc_country_not_found;
        l_text := dwh_constants.vc_country_not_found||' '||g_rec_out.origin_country_code;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
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

   insert into stg_ast_lev1_diff1_hsp values g_rec_in;
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
       insert into fnd_ast_lev1_diff1 values a_tbl_insert(i);

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
       update fnd_ast_lev1_diff1
          set style_no                  =  a_tbl_update(i).style_no,
              subclass_no               =  a_tbl_update(i).subclass_no,
              class_no                  =  a_tbl_update(i).class_no,
              department_no             =  a_tbl_update(i).department_no,
              subgroup_no               =  a_tbl_update(i).subgroup_no,
              group_no                  =  a_tbl_update(i).group_no,
              business_unit_no          =  a_tbl_update(i).business_unit_no,
              company_no                =  a_tbl_update(i).company_no,
              merch_season_no           =  a_tbl_update(i).merch_season_no,
              primary_supplier_no       =  a_tbl_update(i).primary_supplier_no,
--              diff_range_no             =  a_tbl_update(i).diff_range_no,
              purchase_type_no          =  a_tbl_update(i).purchase_type_no,
              style_colour_desc         =  a_tbl_update(i).style_colour_desc,
              style_desc                =  a_tbl_update(i).style_desc,
              diff_1_code               =  a_tbl_update(i).diff_1_code,
              into_store_fin_week_code  =  a_tbl_update(i).into_store_fin_week_code,
              time_on_offer             =  a_tbl_update(i).time_on_offer,
              rpl_ind                   =  a_tbl_update(i).rpl_ind,
              origin_country_code       =  a_tbl_update(i).origin_country_code,
              supply_chain_code         =  a_tbl_update(i).supply_chain_code,
              size_curve_no             =  a_tbl_update(i).size_curve_no,
              size_curve_desc           =  a_tbl_update(i).size_curve_desc,
              last_updated_date         =  a_tbl_update(i).last_updated_date
        where style_colour_no           =  a_tbl_update(i).style_colour_no;

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

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin

    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_ast_lev1_diff1_cpy
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
   -- Check to see if it is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_ast_lev1_diff1
   where  style_colour_no         =  g_rec_out.style_colour_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item uda already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).style_colour_no         = g_rec_out.style_colour_no then

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

    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOADING FND_AST_LEV1_DIFF1 DATA STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
-- Retrieve restructure indicator from dim_control
--**************************************************************************************************
    select restructure_ind
    into   g_restructure_ind
    from   dim_control;

    l_text := 'RESTRUCTURE_IND IS:- '||g_restructure_ind;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_bam_lev1_diff1;
    fetch c_bam_lev1_diff1 bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_bam_lev1_diff1 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_bam_lev1_diff1;
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
end WH_FND_AST_009U;
