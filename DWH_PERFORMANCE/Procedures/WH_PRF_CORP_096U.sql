--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_096U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_096U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        November 2008
--  Author:      Alastair de Wet
--  Purpose:     Create planning_only ind in the performance layer
--               on merch hierachy tables.
--  Tables:      Input  - dim item thru dim group,
--               Output - dim_subclass thru dim bu
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
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
g_count              integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_pl_ind             number        :=  0;
g_hc_ind             number        :=  0;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_096U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE PLANNING & HAS CHILDREN IND ON MERCH HIERACHY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_dim_subclass is
   select sk1_subclass_no
   from   dim_subclass ;

cursor c_dim_class is
   select sk1_class_no
   from   dim_class ;

cursor c_dim_department is
   select sk1_department_no
   from   dim_department ;

cursor c_dim_subgroup is
   select sk1_subgroup_no
   from   dim_subgroup  ;

cursor c_dim_group is
   select sk1_group_no
   from   dim_group  ;

cursor c_dim_business_unit is
   select sk1_business_unit_no
   from   dim_business_unit ;

g_sc                c_dim_subclass%rowtype;
g_cl                c_dim_class%rowtype;
g_dp                c_dim_department%rowtype;
g_sg                c_dim_subgroup%rowtype;
g_gr                c_dim_group%rowtype;
g_bu                c_dim_business_unit%rowtype;



--**************************************************************************************************
-- Write valid data out to the merch hierachy tables
--**************************************************************************************************
procedure local_write_subclass as

begin
    for v_dim_subclass in c_dim_subclass
    loop
      g_hc_ind := 1;
      g_recs_read := g_recs_read + 1;
      g_sc := v_dim_subclass;

--QC 206 new code
      select count(*)
      into  g_count
      from  dim_item di,dim_item_uda diu
      where sk1_subclass_no = g_sc.sk1_subclass_no and
            di.item_no      = diu.item_no and
            diu.planning_item_ind_2901 <> '1';
      if g_count = 0 then
         g_pl_ind := 1;
      else
         g_pl_ind := 0;
      end if;
--QC 206 new code end

      select count(*)
      into  g_count
      from  dim_item
      where sk1_subclass_no = g_sc.sk1_subclass_no;
      if g_count = 0 then
--QC 206 old code         g_pl_ind := 1;
         g_hc_ind := 0;
      end if;

      update dim_subclass
      set    planning_only_ind          = g_pl_ind,
             hierarchy_has_children_ind = g_hc_ind,
             last_updated_date          = g_date
      where  sk1_subclass_no            = g_sc.sk1_subclass_no;

      g_recs_updated := g_recs_updated + sql%rowcount;
   end loop;

   exception

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_write_subclass;
--**************************************************************************************************
procedure local_write_class as

begin
    for v_dim_class in c_dim_class
    loop
      g_pl_ind := 0;
      g_hc_ind := 1;
      g_recs_read := g_recs_read + 1;
      g_cl := v_dim_class;

      select count(*)
      into  g_count
      from  dim_subclass
      where sk1_class_no       = g_cl.sk1_class_no and
            planning_only_ind  =  0;

      if g_count = 0 then
         g_pl_ind := 1;
      end if;

      select count(*)
      into  g_count
      from  dim_subclass
      where sk1_class_no                = g_cl.sk1_class_no and
            hierarchy_has_children_ind  =  1;

      if g_count = 0 then
         g_hc_ind := 0;
      end if;

      update dim_class
      set    planning_only_ind          = g_pl_ind,
             hierarchy_has_children_ind = g_hc_ind,
             last_updated_date          = g_date
      where  sk1_class_no               = g_cl.sk1_class_no ;

      g_recs_updated := g_recs_updated + sql%rowcount;
   end loop;

   exception

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_write_class;
--**************************************************************************************************
procedure local_write_department as

begin
    for v_dim_department in c_dim_department
    loop
      g_pl_ind := 0;
      g_hc_ind := 1;
      g_recs_read := g_recs_read + 1;
      g_dp := v_dim_department;

      select count(*)
      into  g_count
      from  dim_class
      where sk1_department_no  = g_dp.sk1_department_no and
            planning_only_ind  = 0;

      if g_count = 0 then
         g_pl_ind := 1;
      end if;

      select count(*)
      into  g_count
      from  dim_class
      where sk1_department_no           = g_dp.sk1_department_no and
            hierarchy_has_children_ind  = 1;

      if g_count = 0 then
         g_hc_ind := 0;
      end if;

      update dim_department
      set    planning_only_ind          = g_pl_ind,
             hierarchy_has_children_ind = g_hc_ind,
             last_updated_date          = g_date
      where  sk1_department_no               = g_dp.sk1_department_no ;

      g_recs_updated := g_recs_updated + sql%rowcount;
   end loop;

   exception

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_write_department;
--**************************************************************************************************
procedure local_write_subgroup as

begin
    for v_dim_subgroup in c_dim_subgroup
    loop
      g_pl_ind := 0;
      g_hc_ind := 1;
      g_recs_read := g_recs_read + 1;
      g_sg := v_dim_subgroup;

      select count(*)
      into  g_count
      from  dim_department
      where sk1_subgroup_no    = g_sg.sk1_subgroup_no and
            planning_only_ind  = 0;

      if g_count = 0 then
         g_pl_ind := 1;
      end if;

      select count(*)
      into  g_count
      from  dim_department
      where sk1_subgroup_no             = g_sg.sk1_subgroup_no and
            hierarchy_has_children_ind  = 1;

      if g_count = 0 then
         g_hc_ind := 0;
      end if;

      update dim_subgroup
      set    planning_only_ind          = g_pl_ind,
             hierarchy_has_children_ind = g_hc_ind,
             last_updated_date          = g_date
      where  sk1_subgroup_no            = g_sg.sk1_subgroup_no ;

      g_recs_updated := g_recs_updated + sql%rowcount;
   end loop;

   exception

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_write_subgroup;
--**************************************************************************************************
procedure local_write_group as

begin
    for v_dim_group in c_dim_group
    loop
      g_pl_ind := 0;
      g_hc_ind := 1;
      g_recs_read := g_recs_read + 1;
      g_gr := v_dim_group;

      select count(*)
      into  g_count
      from  dim_subgroup
      where sk1_group_no       = g_gr.sk1_group_no and
            planning_only_ind  = 0;

      if g_count = 0 then
         g_pl_ind := 1;
      end if;

      select count(*)
      into  g_count
      from  dim_subgroup
      where sk1_group_no                = g_gr.sk1_group_no and
            hierarchy_has_children_ind  = 1;

      if g_count = 0 then
         g_hc_ind := 0;
      end if;

      update dim_group
      set    planning_only_ind          = g_pl_ind,
             hierarchy_has_children_ind = g_hc_ind,
             last_updated_date          = g_date
      where  sk1_group_no            = g_gr.sk1_group_no ;

      g_recs_updated := g_recs_updated + sql%rowcount;
   end loop;

   exception

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_write_group;
--**************************************************************************************************
procedure local_write_business_unit as

begin
    for v_dim_business_unit in c_dim_business_unit
    loop
      g_pl_ind := 0;
      g_hc_ind := 1;
      g_recs_read := g_recs_read + 1;
      g_bu := v_dim_business_unit;

      select count(*)
      into  g_count
      from  dim_group
      where sk1_business_unit_no = g_bu.sk1_business_unit_no and
            planning_only_ind    = 0;

      if g_count = 0 then
         g_pl_ind := 1;
      end if;

      select count(*)
      into  g_count
      from  dim_group
      where sk1_business_unit_no        = g_bu.sk1_business_unit_no and
            hierarchy_has_children_ind  = 1;

      if g_count = 0 then
         g_hc_ind := 0;
      end if;

      update dim_business_unit
      set    planning_only_ind          = g_pl_ind,
             hierarchy_has_children_ind = g_hc_ind,
             last_updated_date          = g_date
      where  sk1_business_unit_no       = g_bu.sk1_business_unit_no ;

      g_recs_updated := g_recs_updated + sql%rowcount;
   end loop;

   exception

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_write_business_unit;




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

    l_text := 'CREATE PLANNING & HAS CHILDREN IND MERCH HIERACHY STARTED AT '||
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
    local_write_subclass;
    l_text :=  dwh_constants.vc_log_records_read||' - SUBCLASS:- '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_recs_read := 0 ;
    local_write_class;
    l_text :=  dwh_constants.vc_log_records_read||' - CLASS:- '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_recs_read := 0;
    local_write_department;
    l_text :=  dwh_constants.vc_log_records_read||' - DEPARTMENT:- '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_recs_read := 0 ;
    local_write_subgroup;
    l_text :=  dwh_constants.vc_log_records_read||' - SUBGROUP:- '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_recs_read := 0 ;
    local_write_group;
    l_text :=  dwh_constants.vc_log_records_read||' - GROUP:- '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_recs_read := 0;
    local_write_business_unit;
    l_text :=  dwh_constants.vc_log_records_read||' - BUSINESS UNIT:- '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_recs_read := 0 ;
--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
end wh_prf_corp_096u;
