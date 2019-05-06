--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_743U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_743U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        February 2013
--  Author:      Quentin Smit
--  Purpose:     Create zone_item_supp dimention table in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - stg_jdaff_po_plan
--               Output - fnd_zone_item_supp_ff_po_plan
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  22 Feb 2016  B Kirschner - add source col FROM_LOC_NO to corresponding col in target table Ref: BK22FEB16
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria
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
g_hospital_text      stg_jdaff_po_plan_hsp.sys_process_msg%type;
g_rec_out            fnd_zone_item_supp_ff_po_plan%rowtype;
g_rec_in             stg_jdaff_po_plan_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_count              integer       :=  0;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_743U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE_ITEM_SUPP MASTERDATA EX JDAFF';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_jdaff_po_plan_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_zone_item_supp_ff_po_plan%rowtype index by binary_integer;
type tbl_array_u is table of fnd_zone_item_supp_ff_po_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_jdaff_po_plan.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_jdaff_po_plan.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;
l_day_no            integer;
l_day01_cases       number(14,2);
l_day02_cases       number(14,2);
l_day03_cases       number(14,2);
l_day04_cases       number(14,2);
l_day05_cases       number(14,2);
l_day06_cases       number(14,2);
l_cnt               integer;

cursor c_stg_jdaff_po_plan is
   select a.*
   from stg_jdaff_po_plan_cpy a
--   , fnd_jdaff_dept_rollout b, dim_item c
   where sys_process_code = 'N'
 --    and a.item_no = c.item_no
--     and c.department_no = b.department_no
--     and b.department_live_ind = 'Y'
   order by sys_source_batch_id,sys_source_sequence_no;

   --where sys_process_code = 'N'
   --order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                               := 'N';

    g_rec_out.zone_group_no                   := 1;   --g_rec_in.zone_group_no;
    g_rec_out.zone_no                         := g_rec_in.zone_no;
    g_rec_out.item_no                         := g_rec_in.item_no;
    g_rec_out.supplier_no                     := g_rec_in.supplier_no;
    g_rec_out.from_loc_no                     := g_rec_in.from_loc_no;                  -- BK22FEB16
    g_rec_out.to_loc_no                       := g_rec_in.to_loc_no;                    -- BK22FEB16
    g_rec_out.dc_supp_inbound_cases           := g_rec_in.dc_supp_inbound_cases;
    g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
    g_rec_out.CALENDAR_DATE                   := g_rec_in.calendar_date;

 --   l_text := 'CALENDAR DATE =  '|| g_rec_in.calendar_date;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --   l_text := 'ITEM =  '|| g_rec_in.item_no;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select fin_day_no
      into l_day_no
      from dim_calendar
     where calendar_date = g_rec_in.calendar_date;

--   l_text := 'l_day_no =  '|| l_day_no;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --   l_text := 'item_no =  '|| g_rec_out.item_no;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
/*
   select day01_cases into l_day01_cases
     from fnd_zone_item_supp_ff_po_plan
    where zone_group_no =1
      and zone_no = g_rec_out.zone_no
      and item_no = g_rec_out.item_no
      and supplier_no = g_rec_out.supplier_no
      and calendar_date = g_rec_out.calendar_date-1;

   select day02_cases into l_day02_cases
     from fnd_zone_item_supp_ff_po_plan
    where zone_group_no =1
      and zone_no = g_rec_out.zone_no
      and item_no = g_rec_out.item_no
      and supplier_no = g_rec_out.supplier_no
      and calendar_date = g_rec_out.calendar_date-2;

   select day03_cases into l_day03_cases
     from fnd_zone_item_supp_ff_po_plan
    where zone_group_no =1
      and zone_no = g_rec_out.zone_no
      and item_no = g_rec_out.item_no
      and supplier_no = g_rec_out.supplier_no
      and calendar_date = g_rec_out.calendar_date-3;

   select day04_cases into l_day04_cases
     from fnd_zone_item_supp_ff_po_plan
    where zone_group_no =1
      and zone_no = g_rec_out.zone_no
      and item_no = g_rec_out.item_no
      and supplier_no = g_rec_out.supplier_no
      and calendar_date = g_rec_out.calendar_date-4;

   select day05_cases into l_day05_cases
     from fnd_zone_item_supp_ff_po_plan
    where zone_group_no =1
      and zone_no = g_rec_out.zone_no
      and item_no = g_rec_out.item_no
      and supplier_no = g_rec_out.supplier_no
      and calendar_date = g_rec_out.calendar_date-5;

   select day06_cases into l_day06_cases
     from fnd_zone_item_supp_ff_po_plan
    where zone_group_no =1
      and zone_no = g_rec_out.zone_no
      and item_no = g_rec_out.item_no
      and supplier_no = g_rec_out.supplier_no
      and calendar_date = g_rec_out.calendar_date-6;
 */
   --l_text := 'l_day01_cases = ' || l_day01_cases;
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   case l_day_no
     when 1 then
        g_rec_out.day01_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.day02_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.day03_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.day04_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.day05_cases     := g_rec_in.week_1_day_5_cases;
        g_rec_out.day06_cases     := g_rec_in.week_1_day_6_cases;
        g_rec_out.day07_cases     := g_rec_in.week_1_day_7_cases;

        g_rec_out.day08_cases     := g_rec_in.week_2_day_1_cases;
        g_rec_out.day09_cases     := g_rec_in.week_2_day_2_cases;
        g_rec_out.day10_cases     := g_rec_in.week_2_day_3_cases;
        g_rec_out.day11_cases     := g_rec_in.week_2_day_4_cases;
        g_rec_out.day12_cases     := g_rec_in.week_2_day_5_cases;
        g_rec_out.day13_cases     := g_rec_in.week_2_day_6_cases;
        g_rec_out.day14_cases     := g_rec_in.week_2_day_7_cases;

        g_rec_out.day15_cases     := g_rec_in.week_3_day_1_cases;
        g_rec_out.day16_cases     := g_rec_in.week_3_day_2_cases;
        g_rec_out.day17_cases     := g_rec_in.week_3_day_3_cases;
        g_rec_out.day18_cases     := g_rec_in.week_3_day_4_cases;
        g_rec_out.day19_cases     := g_rec_in.week_3_day_5_cases;
        g_rec_out.day20_cases     := g_rec_in.week_3_day_6_cases;
        g_rec_out.day21_cases     := g_rec_in.week_3_day_7_cases;

    when 2 then
        --l_text := 'HERE!!' || l_day_no;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        --l_text := 'g_rec_out.day01_cases (1) =  '|| g_rec_out.day01_cases;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-1;

        if l_cnt > 0 then
           select day01_cases into l_day01_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no       = g_rec_out.zone_no
              and item_no       = g_rec_out.item_no
              and supplier_no   = g_rec_out.supplier_no
             and calendar_date  = g_rec_out.calendar_date-1;

             g_rec_out.day01_cases     := l_day01_cases;
        else
             g_rec_out.day01_cases     := 0;
        end if;

        g_rec_out.day01_cases     := l_day01_cases;
        g_rec_out.day02_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.day03_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.day04_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.day05_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.day06_cases     := g_rec_in.week_1_day_5_cases;
        g_rec_out.day07_cases     := g_rec_in.week_1_day_6_cases;
        g_rec_out.day08_cases     := g_rec_in.week_1_day_7_cases;
        g_rec_out.day09_cases     := g_rec_in.week_2_day_1_cases;
        g_rec_out.day10_cases     := g_rec_in.week_2_day_2_cases;
        g_rec_out.day11_cases     := g_rec_in.week_2_day_3_cases;
        g_rec_out.day12_cases     := g_rec_in.week_2_day_4_cases;
        g_rec_out.day13_cases     := g_rec_in.week_2_day_5_cases;
        g_rec_out.day14_cases     := g_rec_in.week_2_day_6_cases;
        g_rec_out.day15_cases     := g_rec_in.week_2_day_7_cases;
        g_rec_out.day16_cases     := g_rec_in.week_3_day_1_cases;
        g_rec_out.day17_cases     := g_rec_in.week_3_day_2_cases;
        g_rec_out.day18_cases     := g_rec_in.week_3_day_3_cases;
        g_rec_out.day19_cases     := g_rec_in.week_3_day_4_cases;
        g_rec_out.day20_cases     := g_rec_in.week_3_day_5_cases;
        g_rec_out.day21_cases     := g_rec_in.week_3_day_6_cases;


      when 3 then

         --l_text := 'DAY 3!!' ;
         --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

         select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-1;

           --l_text := 'day2 count (calendar date - 1) =  '|| l_cnt;
           --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        if l_cnt > 0 then
           select day02_cases into l_day02_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no       = g_rec_out.zone_no
              and item_no       = g_rec_out.item_no
              and supplier_no   = g_rec_out.supplier_no
             and calendar_date  = g_rec_out.calendar_date-1;

             g_rec_out.day02_cases     := l_day02_cases;
        else
             g_rec_out.day02_cases     := 0;
        end if;
        --l_text := 'g_rec_out.day02_case =  '|| g_rec_out.day02_cases;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

         select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-2;

           --l_text := 'day1 count (calendar date - 2) =  '|| l_cnt;
           --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        if l_cnt > 0 then
           select day01_cases into l_day01_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-2;

           g_rec_out.day01_cases     := l_day01_cases;
        else
           g_rec_out.day01_cases     := 0;
        end if;
        --l_text := 'g_rec_out.day01_cases =  '|| g_rec_out.day01_cases;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        g_rec_out.day03_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.day04_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.day05_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.day06_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.day07_cases     := g_rec_in.week_1_day_5_cases;
        g_rec_out.day08_cases     := g_rec_in.week_1_day_6_cases;
        g_rec_out.day09_cases     := g_rec_in.week_1_day_7_cases;
        g_rec_out.day10_cases    := g_rec_in.week_2_day_1_cases;
        g_rec_out.day11_cases    := g_rec_in.week_2_day_2_cases;
        g_rec_out.day12_cases    := g_rec_in.week_2_day_3_cases;
        g_rec_out.day13_cases    := g_rec_in.week_2_day_4_cases;
        g_rec_out.day14_cases    := g_rec_in.week_2_day_5_cases;
        g_rec_out.day15_cases    := g_rec_in.week_2_day_6_cases;
        g_rec_out.day16_cases    := g_rec_in.week_2_day_7_cases;
        g_rec_out.day17_cases    := g_rec_in.week_3_day_1_cases;
        g_rec_out.day18_cases    := g_rec_in.week_3_day_2_cases;
        g_rec_out.day19_cases    := g_rec_in.week_3_day_3_cases;
        g_rec_out.day20_cases    := g_rec_in.week_3_day_4_cases;
        g_rec_out.day21_cases    := g_rec_in.week_3_day_5_cases;


    when 4 then

   -- l_text := 'DAY 4!!' ;
   --      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-1;

        if l_cnt > 0 then
           select day01_cases into l_day03_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no       = g_rec_out.zone_no
              and item_no       = g_rec_out.item_no
              and supplier_no   = g_rec_out.supplier_no
             and calendar_date  = g_rec_out.calendar_date-1;

             g_rec_out.day03_cases     := l_day03_cases;
        else
             g_rec_out.day03_cases     := 0;
        end if;

         select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-2;

        if l_cnt > 0 then
           select day02_cases into l_day02_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-2;

           g_rec_out.day02_cases     := l_day02_cases;
        else
           g_rec_out.day02_cases     := 0;
        end if;

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-3;

        if l_cnt > 0 then
           select day01_cases into l_day01_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-3;

           g_rec_out.day01_cases     := l_day01_cases;
        else
           g_rec_out.day01_cases     := 0;
        end if;

        --l_text := 'g_rec_out.day01_cases =  '|| g_rec_out.day01_cases;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        --l_text := 'g_rec_out.day02_cases =  '|| g_rec_out.day02_cases;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        --l_text := 'g_rec_out.day03_cases =  '|| g_rec_out.day03_cases;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        g_rec_out.day04_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.day05_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.day06_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.day07_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.day08_cases     := g_rec_in.week_1_day_5_cases;
        g_rec_out.day09_cases     := g_rec_in.week_1_day_6_cases;
        g_rec_out.day10_cases    := g_rec_in.week_1_day_7_cases;
        g_rec_out.day11_cases    := g_rec_in.week_2_day_1_cases;
        g_rec_out.day12_cases    := g_rec_in.week_2_day_2_cases;
        g_rec_out.day13_cases    := g_rec_in.week_2_day_3_cases;
        g_rec_out.day14_cases    := g_rec_in.week_2_day_4_cases;
        g_rec_out.day15_cases    := g_rec_in.week_2_day_5_cases;
        g_rec_out.day16_cases    := g_rec_in.week_2_day_6_cases;
        g_rec_out.day17_cases    := g_rec_in.week_2_day_7_cases;
        g_rec_out.day18_cases    := g_rec_in.week_3_day_1_cases;
        g_rec_out.day19_cases    := g_rec_in.week_3_day_2_cases;
        g_rec_out.day20_cases    := g_rec_in.week_3_day_3_cases;
        g_rec_out.day21_cases    := g_rec_in.week_3_day_4_cases;


     when 5 then

   --  l_text := 'DAY 5!!' ;
   --  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-1;

        if l_cnt > 0 then
           select day04_cases into l_day04_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no       = g_rec_out.zone_no
              and item_no       = g_rec_out.item_no
              and supplier_no   = g_rec_out.supplier_no
             and calendar_date  = g_rec_out.calendar_date-1;

             g_rec_out.day04_cases     := l_day04_cases;
        else
             g_rec_out.day04_cases     := 0;
        end if;

         select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-2;

        if l_cnt > 0 then
           select day03_cases into l_day03_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-2;

           g_rec_out.day03_cases     := l_day03_cases;
        else
           g_rec_out.day03_cases     := 0;
        end if;

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-3;

        if l_cnt > 0 then
           select day02_cases into l_day02_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-3;

           g_rec_out.day02_cases     := l_day02_cases;
        else
           g_rec_out.day02_cases     := 0;
        end if;

   select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-4;

        if l_cnt > 0 then
           select day04_cases into l_day01_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-4;

           g_rec_out.day01_cases     := l_day01_cases;
        else
           g_rec_out.day01_cases     := 0;
        end if;


        g_rec_out.day05_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.day06_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.day07_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.day08_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.day09_cases     := g_rec_in.week_1_day_5_cases;
        g_rec_out.day10_cases    := g_rec_in.week_1_day_6_cases;
        g_rec_out.day11_cases    := g_rec_in.week_1_day_7_cases;
        g_rec_out.day12_cases    := g_rec_in.week_2_day_1_cases;
        g_rec_out.day13_cases    := g_rec_in.week_2_day_2_cases;
        g_rec_out.day14_cases    := g_rec_in.week_2_day_3_cases;
        g_rec_out.day15_cases    := g_rec_in.week_2_day_4_cases;
        g_rec_out.day16_cases    := g_rec_in.week_2_day_5_cases;
        g_rec_out.day17_cases    := g_rec_in.week_2_day_6_cases;
        g_rec_out.day18_cases    := g_rec_in.week_2_day_7_cases;
        g_rec_out.day19_cases    := g_rec_in.week_3_day_1_cases;
        g_rec_out.day20_cases    := g_rec_in.week_3_day_2_cases;
        g_rec_out.day21_cases    := g_rec_in.week_3_day_3_cases;


     when 6 then

    --l_text := 'DAY 6!!' ;
    -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-1;

        if l_cnt > 0 then
           select day05_cases into l_day05_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no       = g_rec_out.zone_no
              and item_no       = g_rec_out.item_no
              and supplier_no   = g_rec_out.supplier_no
             and calendar_date  = g_rec_out.calendar_date-1;

             g_rec_out.day05_cases     := l_day05_cases;
        else
             g_rec_out.day05_cases     := 0;
        end if;

         select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-2;

        if l_cnt > 0 then
           select day04_cases into l_day04_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-2;

           g_rec_out.day04_cases     := l_day04_cases;
        else
           g_rec_out.day04_cases     := 0;
        end if;

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-3;

        if l_cnt > 0 then
           select day03_cases into l_day03_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-3;

           g_rec_out.day03_cases     := l_day03_cases;
        else
           g_rec_out.day03_cases     := 0;
        end if;

   select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-4;

        if l_cnt > 0 then
           select day02_cases into l_day02_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-4;

           g_rec_out.day02_cases     := l_day02_cases;
        else
           g_rec_out.day02_cases     := 0;
        end if;

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-5;

        if l_cnt > 0 then
           select day01_cases into l_day01_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-5;

           g_rec_out.day01_cases     := l_day01_cases;
        else
           g_rec_out.day01_cases     := 0;
        end if;

     --l_text := 'g_rec_out.day21_cases (1)= ' || g_rec_out.day21_cases;
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        g_rec_out.day06_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.day07_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.day08_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.day09_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.day10_cases    := g_rec_in.week_1_day_5_cases;
        g_rec_out.day11_cases    := g_rec_in.week_1_day_6_cases;
        g_rec_out.day12_cases    := g_rec_in.week_1_day_7_cases;
        g_rec_out.day13_cases    := g_rec_in.week_2_day_1_cases;
        g_rec_out.day14_cases    := g_rec_in.week_2_day_2_cases;
        g_rec_out.day15_cases    := g_rec_in.week_2_day_3_cases;
        g_rec_out.day16_cases    := g_rec_in.week_2_day_4_cases;
        g_rec_out.day17_cases    := g_rec_in.week_2_day_5_cases;
        g_rec_out.day18_cases    := g_rec_in.week_2_day_6_cases;
        g_rec_out.day19_cases    := g_rec_in.week_2_day_7_cases;
        g_rec_out.day20_cases    := g_rec_in.week_3_day_1_cases;
        g_rec_out.day21_cases    := g_rec_in.week_3_day_2_cases;

    --    l_text := 'g_rec_out.day21_cases (2)= ' || g_rec_out.day21_cases;
    -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    -- l_text := 'g_rec_out.day21_cases (2)= ' || g_rec_out.day21_cases;
    -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     when 7 then

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-1;

        if l_cnt > 0 then
           select day06_cases into l_day06_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no       = g_rec_out.zone_no
              and item_no       = g_rec_out.item_no
              and supplier_no   = g_rec_out.supplier_no
             and calendar_date  = g_rec_out.calendar_date-1;

             g_rec_out.day06_cases     := l_day06_cases;
        else
             g_rec_out.day06_cases     := 0;
        end if;

         select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no  =1
           and zone_no        = g_rec_out.zone_no
           and item_no        = g_rec_out.item_no
           and supplier_no    = g_rec_out.supplier_no
           and calendar_date  = g_rec_out.calendar_date-2;

        if l_cnt > 0 then
           select day05_cases into l_day05_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-2;

           g_rec_out.day05_cases     := l_day05_cases;
        else
           g_rec_out.day05_cases     := 0;
        end if;

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-3;

        if l_cnt > 0 then
           select day04_cases into l_day04_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-3;

           g_rec_out.day04_cases     := l_day04_cases;
        else
           g_rec_out.day04_cases     := 0;
        end if;

   select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-4;

        if l_cnt > 0 then
           select day03_cases into l_day03_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-4;

           g_rec_out.day03_cases     := l_day03_cases;
        else
           g_rec_out.day03_cases     := 0;
        end if;

        select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-5;

        if l_cnt > 0 then
           select day02_cases into l_day02_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-5;

           g_rec_out.day02_cases     := l_day02_cases;
        else
           g_rec_out.day02_cases     := 0;
        end if;

         select count(*) into l_cnt
          from fnd_zone_item_supp_ff_po_plan
         where zone_group_no =1
           and zone_no = g_rec_out.zone_no
           and item_no = g_rec_out.item_no
           and supplier_no = g_rec_out.supplier_no
           and calendar_date = g_rec_out.calendar_date-6;

        if l_cnt > 0 then
           select day06_cases into l_day01_cases
             from fnd_zone_item_supp_ff_po_plan
            where zone_group_no =1
              and zone_no = g_rec_out.zone_no
              and item_no = g_rec_out.item_no
              and supplier_no = g_rec_out.supplier_no
              and calendar_date = g_rec_out.calendar_date-6;

           g_rec_out.day01_cases     := l_day01_cases;
        else
           g_rec_out.day01_cases     := 0;
        end if;

        g_rec_out.day07_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.day08_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.day09_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.day10_cases    := g_rec_in.week_1_day_4_cases;
        g_rec_out.day11_cases    := g_rec_in.week_1_day_5_cases;
        g_rec_out.day12_cases    := g_rec_in.week_1_day_6_cases;
        g_rec_out.day13_cases    := g_rec_in.week_1_day_7_cases;
        g_rec_out.day14_cases    := g_rec_in.week_2_day_1_cases;
        g_rec_out.day15_cases    := g_rec_in.week_2_day_2_cases;
        g_rec_out.day16_cases    := g_rec_in.week_2_day_3_cases;
        g_rec_out.day17_cases    := g_rec_in.week_2_day_4_cases;
        g_rec_out.day18_cases    := g_rec_in.week_2_day_5_cases;
        g_rec_out.day19_cases    := g_rec_in.week_2_day_6_cases;
        g_rec_out.day20_cases    := g_rec_in.week_2_day_7_cases;
        g_rec_out.day21_cases    := g_rec_in.week_3_day_1_cases;


   end case;

    g_rec_out.last_updated_date               := g_date;


   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_source_code;
   end if;


--   if not  dwh_valid.fnd_zone(g_rec_out.zone_no,g_rec_out.zone_group_no) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_zone_not_found;
--     l_text          := dwh_constants.vc_zone_not_found||g_rec_out.zone_no||' '||g_rec_out.zone_group_no  ;
--     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--     return;
--   end if;

   if not  dwh_valid.fnd_item(g_rec_out.item_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_item_not_found;
     l_text          := dwh_constants.vc_item_not_found||g_rec_out.item_no;
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

   insert into stg_jdaff_po_plan_hsp values g_rec_in;
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
       insert into fnd_zone_item_supp_ff_po_plan values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).item_no||
                       ' '||a_tbl_insert(g_error_index).supplier_no||
                       ' '||a_tbl_insert(g_error_index).calendar_date;
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
       update fnd_zone_item_supp_ff_po_plan
       set    from_loc_no                     = a_tbl_update(i).from_loc_no,
              to_loc_no                       = a_tbl_update(i).to_loc_no,
              dc_supp_inbound_cases           = a_tbl_update(i).dc_supp_inbound_cases,
              day01_cases                      = a_tbl_update(i).day01_cases,
              day02_cases                      = a_tbl_update(i).day02_cases,
              day03_cases                      = a_tbl_update(i).day03_cases,
              day04_cases                      = a_tbl_update(i).day04_cases,
              day05_cases                      = a_tbl_update(i).day05_cases,
              day06_cases                      = a_tbl_update(i).day06_cases,
              day07_cases                      = a_tbl_update(i).day07_cases,
              day08_cases                      = a_tbl_update(i).day08_cases,
              day09_cases                      = a_tbl_update(i).day09_cases,
              day10_cases                     = a_tbl_update(i).day10_cases,
              day11_cases                     = a_tbl_update(i).day11_cases,
              day12_cases                     = a_tbl_update(i).day12_cases,
              day13_cases                     = a_tbl_update(i).day13_cases,
              day14_cases                     = a_tbl_update(i).day14_cases,
              day15_cases                     = a_tbl_update(i).day15_cases,
              day16_cases                     = a_tbl_update(i).day16_cases,
              day17_cases                     = a_tbl_update(i).day17_cases,
              day18_cases                     = a_tbl_update(i).day18_cases,
              day19_cases                     = a_tbl_update(i).day19_cases,
              day20_cases                     = a_tbl_update(i).day20_cases,
              day21_cases                     = a_tbl_update(i).day21_cases,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  zone_no                         = a_tbl_update(i).zone_no and
              zone_group_no                   = a_tbl_update(i).zone_group_no and
              item_no                         = a_tbl_update(i).item_no and
              supplier_no                     = a_tbl_update(i).supplier_no and
              calendar_date                   = a_tbl_update(i).calendar_date;

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
                       ' '||a_tbl_update(g_error_index).item_no||
                       ' '||a_tbl_update(g_error_index).supplier_no;
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
       update stg_jdaff_po_plan_cpy
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
   from   fnd_zone_item_supp_ff_po_plan
   where  zone_no       = g_rec_out.zone_no and
          zone_group_no = g_rec_out.zone_group_no and
          item_no       = g_rec_out.item_no and
          supplier_no   = g_rec_out.supplier_no and
          calendar_date = g_rec_out.calendar_date;

  if g_count = 1 then
     g_found := TRUE;
  end if;


-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).zone_no         = g_rec_out.zone_no and
            a_tbl_insert(i).zone_group_no   = g_rec_out.zone_group_no and
            a_tbl_insert(i).item_no         = g_rec_out.item_no and
            a_tbl_insert(i).supplier_no     = g_rec_out.supplier_no and
            a_tbl_insert(i).calendar_date   = g_rec_out.calendar_date then
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

    l_text := 'LOAD OF fnd_zone_item_supp_ff_po_plan EX JDAFF STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '06/MAR/14';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_jdaff_po_plan;
    fetch c_stg_jdaff_po_plan bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_jdaff_po_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_jdaff_po_plan;
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
end wh_fnd_corp_743u;
