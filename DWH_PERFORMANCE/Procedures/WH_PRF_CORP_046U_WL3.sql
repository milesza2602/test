--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_046U_WL3
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_046U_WL3" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Location Item fact table in the performance layer
--               with input ex RMS fnd_location_item table from foundation layer.
--  Tables:      Input  - fnd_location_item
--               Output - rtl_location_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  30 Jan 2009 - Defect 491 - remove primary_country_code and replace with sk1_primary_country_code
--                             remove primary_supplier_no and replace with sk1_primary_supplier_no
--
--  09 May 2011 - (NJ) Defect 4282 - added a field called sale_alert_ind.
--  19 May 2011 - Defect 2981 - Add a new measure to be derived (min_shelf_life)
--                            - Add base measures min_shelf_life_tolerance & max_shelf_life_tolerance
--
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
g_recs_tol           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_046U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_location_item%rowtype index by binary_integer;
type tbl_array_u is table of rtl_location_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

g_from_loc_no       integer;
g_to_loc_no         integer;


cursor c_fnd_shelf_life_tol is
   select li.sk1_item_no,
          li.sk1_location_no,
          nvl(di.min_shelf_life_tolerance,0) min_shelf_life_tolerance,
          nvl(di.max_shelf_life_tolerance,0) max_shelf_life_tolerance,
          case
          when   nvl(li.num_shelf_life_days,0) = 0 then 0
          when   di.min_shelf_life_tolerance is null then nvl(li.num_shelf_life_days,0)
          else   li.num_shelf_life_days - di.min_shelf_life_tolerance
          end as min_sl
   from   rtl_location_item li,
          dim_item di
   where  li.sk1_item_no             = di.sk1_item_no  and
          di.business_unit_no        = 50 and
           (li.min_shelf_life_tolerance       <> nvl(di.min_shelf_life_tolerance,0) or
            li.max_shelf_life_tolerance       <> nvl(di.max_shelf_life_tolerance,0) or
            case
            when   nvl(li.num_shelf_life_days,0) = 0 then 0
            else   li.num_shelf_life_days - nvl(di.min_shelf_life_tolerance,0)  
            end    <> li.min_shelf_life or
            li.min_shelf_life_tolerance       is null or
            li.max_shelf_life_tolerance       is null or
            li.min_shelf_life                 is null) ;


--**************************************************************************************************
-- Update the Min &Max shelf life days ex dim_item
--**************************************************************************************************
procedure shelf_life_days_update as
begin


   for shelf_life_record in c_fnd_shelf_life_tol
   loop
     update rtl_location_item
     set    min_shelf_life_tolerance        = shelf_life_record.min_shelf_life_tolerance,
            max_shelf_life_tolerance        = shelf_life_record.max_shelf_life_tolerance,
            min_shelf_life                  = shelf_life_record.min_sl,
            last_updated_date               = g_date
     where  sk1_item_no                     = shelf_life_record.sk1_item_no   and
            sk1_location_no                 = shelf_life_record.sk1_location_no ;

     g_recs_tol  := g_recs_tol  + sql%rowcount;
   end loop;
   exception
     when others then
       l_message := 'Update error min/max tolerance '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end shelf_life_days_update;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=200000000';
execute immediate 'alter session enable parallel dml';


    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LOCATION_ITEM EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
  
       shelf_life_days_update;


    l_text :=  dwh_constants.vc_log_records_updated||'SL Days ex Item '||g_recs_tol;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    p_success := true;
    commit;
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
end wh_prf_corp_046u_WL3;
