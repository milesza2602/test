--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_248U_WLL4L
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_248U_WLL4L" 
(p_forall_limit in integer,
p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2009
--  Author:      Wendy Lyttle
--  Purpose:     Load like4like ind table in the foundation layer
--               with input ex staging table from an Excel SS ex finance.
--  Tables:      Input  - stg_excel_like_4_like_cpy
--               Output - fnd_rtl_loc_dy_like_4_like
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 July 2009 - defect 2017 - Add field LIKE_FOR_LIKE_ADJ_IND to tables
--                               FND_RTL_LOC_DY_LIKE_4_LIKE and RTL_LOC_DY
--  14 August 2009 - defect 2252 - Ensure that check for valid location_no is
--                                 done in FND and not PRF for Like4Like
--  19 June 2013  - setting all indicators to what the latest version is for each location.
      --  21 June 2013 - release to prd
--
--**************************************************************************************************
-- The processing of the L$L indicators has changed over time.
-- As at today 2013-june-19, we have swopped over from setting the indicators for this_year and the equivalent last_year
-- to setting all indicators for the last 3 years to what the latest version is for each location.
--
-- To cater for receiving any update at any time, the data is processed as follows :
-- 1. Load data from staging
-- 2. Create a temporary table TEMP_L4L containing the latest indicator value per location.
--    This will cover any eventuality such as an old location not coming through anymore
-- 3. Update the last 3 yrs of FND_RTL_LOC_DY_LIKE_4_LIKE (ie. all history) using TEMP_L4L
--
--
--**************************************************************************************************
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
g_recs_zeroised      integer       :=  0;
g_rec_cnt      integer       :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_excel_like_4_like_hsp.sys_process_msg%type;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_fin_year_no        number        :=  0;
g_fin_week_no        number        :=  0;
g_fin_day_no         number        :=  0;
g_ly_fin_year_no     number        :=  0;
g_ly_fin_week_no     number        :=  0;
g_ly_calendar_date  date;
g_max_ty_calendar_date  date;
g_max_ly_calendar_date  date;
g_max_ty_fin_year_no   number := 0;
g_min_ty_calendar_date date;

v_like_for_like_ind  number(1);
v_like_for_like_adj_ind  number(1);


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_248U_WLL4L';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD LIKE FOR LIKE TRANSACTION EX FINANCE SPREADSHEET';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_excel_like_4_like_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713%rowtype index by binary_integer;
type tbl_array_u is table of DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_excel_like_4_like_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_excel_like_4_like_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

-- We receive data for only 1 day of the week but
-- need to explode it to all 7 days of the week.
-- hence the cartesian product

cursor c_stg_excel_like_4_like is
with selext as (
   select SYS_SOURCE_BATCH_ID,
          SYS_SOURCE_SEQUENCE_NO,
          SYS_MIDDLEWARE_BATCH_ID,
          a.source_data_status_code  c_SOURCE_DATA_STATUS_CODE,
          a.location_no,
          a.fin_year_no              ty_fin_year_no,
          a.fin_week_no              ty_fin_week_no,
          a.fin_day_no               ty_fin_day_no,
          c.calendar_date            ty_calendar_date,
          c.ly_calendar_date         ly_calendar_date,
          c.ly_fin_year_no           ly_fin_year_no,
          c.ly_fin_week_no           ly_fin_week_no,
          a.like_for_like_ind        like_for_like_ind
   from DWH_DATAFIX.stg_excel_like_4_like_WL210713 a,
        dim_calendar c
 where c.fin_year_no = a.fin_year_no
     and c.fin_week_no = a.fin_week_no
)
 select   SYS_SOURCE_BATCH_ID,
          SYS_SOURCE_SEQUENCE_NO,
          SYS_MIDDLEWARE_BATCH_ID,
          c_SOURCE_DATA_STATUS_CODE,
          se.location_no,
          ty_fin_year_no,
          ty_fin_week_no,
          ty_fin_day_no,
          ty_calendar_date,
          ly_calendar_date,
          ly_fin_year_no,
          ly_fin_week_no,
          se.like_for_like_ind,
          fnd1.calendar_date         ty_exists,
          fnd2.calendar_date         ly_exists
   from selext se,
        DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713 fnd1,
        DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713 fnd2
 where  se.ty_calendar_date = fnd1.calendar_date(+)
    and se.location_no = fnd1.location_no(+)
    and se.ly_calendar_date = fnd2.calendar_date(+)
    and se.location_no = fnd2.location_no(+)
   order by se.location_no,
            ty_fin_year_no,
            ty_fin_week_no,
            ty_fin_day_no,
            sys_source_batch_id,sys_source_sequence_no;

   g_rec_in c_stg_excel_like_4_like%rowtype;

--**************************************************************************************************
--                           M A I N     P R O C E S S
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_rtl_loc_dy_like_4_like EX POS STARTED AT '||
                                    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    
    dwh_lookup.dim_control(g_date);
 --   g_date := '7 JULY 2013';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

--**************************************************************************************************
-- M A I N   S E C T I O N
--**************************************************************************************************
  open c_stg_excel_like_4_like;
      loop
          fetch c_stg_excel_like_4_like into g_rec_in;
          exit when c_stg_excel_like_4_like%notfound;

   g_recs_read := g_recs_read+ 1;

-------------------------------------------------------------------------------
-- VALIDATION checks on inidicator and location
-------------------------------------------------------------------------------
   g_hospital      := 'N';
   if not dwh_valid.indicator_field(g_rec_in.like_for_like_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;
   if not dwh_valid.fnd_location(g_rec_in.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_in.location_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

-------------------------------------------------------------------------------
-- If Validation checks fail then write to hospital
-------------------------------------------------------------------------------
  if g_hospital = 'Y'
  then
      begin
           insert into stg_excel_like_4_like_hsp values
                (g_rec_in.SYS_SOURCE_BATCH_ID
                ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
                ,sysdate
                ,'Y'
                ,'DWH'
                ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
                ,g_hospital_text
                ,g_rec_in.LOCATION_NO
                ,g_rec_in.LIKE_FOR_LIKE_IND
                ,g_rec_in.c_SOURCE_DATA_STATUS_CODE
                ,g_rec_in.ty_FIN_YEAR_NO
                ,g_rec_in.ty_FIN_WEEK_NO
                ,g_rec_in.ty_FIN_DAY_NO);
           g_recs_hospital := g_recs_hospital + 1;
      exception
          when dwh_errors.e_insert_error then
               l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
               raise;
          when others then
               l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
       end;

    else
-------------------------------------------------------------------------------
-- If Validation checks DO NOT fail then continue
-------------------------------------------------------------------------------
      if g_rec_in.ty_exists is null
         then  
             insert into  DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713
               values(g_rec_in.LOCATION_NO
                     ,g_rec_in.ty_CALENDAR_DATE
                     ,g_rec_in.like_for_like_ind
                     , ''
                     ,g_date
                     ,g_rec_in.like_for_like_ind);
         commit;
         g_recs_inserted := g_recs_inserted + 1;
      else
                    update DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713 fnd
                    set  fnd.like_for_like_ind = g_rec_in.like_for_like_ind,
                         fnd.like_for_like_adj_ind = g_rec_in.like_for_like_ind,
                         fnd.last_updated_date = g_date
                    where fnd.location_no = g_rec_in.location_no
                    and fnd.calendar_date = g_rec_in.ty_calendar_date;
                             commit;
                    g_recs_updated := g_recs_updated + 1;
      end if;
/*      if g_rec_in.ly_exists is null
      then 
          insert into  dwh_foundation.fnd_rtl_loc_dy_like_4_like
               values(g_rec_in.LOCATION_NO
                     ,g_rec_in.ly_CALENDAR_DATE
                     ,g_rec_in.like_for_like_ind
                     , ''
                     ,g_date
                     ,g_rec_in.like_for_like_ind);
         commit;
         g_recs_inserted := g_recs_inserted + 1;
      end if;
*/
      commit;
      end if;
   exit when c_stg_excel_like_4_like%notfound;
 end loop;

-------------------------
--  get most recent calendar_date/location combination
-------------------------
   execute immediate 'truncate table dwh_foundation.temp_l4l';
   L_TEXT := 'truncate table dwh_foundation.temp_l4l';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert into dwh_foundation.temp_l4l
with selext
              as ( select location_no, 
                                     max(calendar_date) calendar_date
                             from DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713
                             group by location_no)
     select a.location_no, 
             se.calendar_date, b.fin_year_no ty_fin_year_no,
             a.like_for_like_ind
     from DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713 a, dim_calendar b, selext se
     where a.calendar_date = se.calendar_date
     and a.location_no = se.location_no
     and b.calendar_date = se.calendar_date
     group by a.location_no, 
             se.calendar_date, 
             b.fin_year_no,
             a.like_for_like_ind
             order by a.location_no
      ;
  g_rec_cnt := SQL%ROWCOUNT;
  l_text := 'Max-calendar recs written to TEMP_L4L = '||g_rec_cnt;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 commit;

-------------------------
--  update from beginning of THIS_FINANCIAL_YEAR using most recent calendar_date/location combination
------------------------- 



 Select max(ty_calendar_date) , max(ty_fin_year_no)
 into g_max_ty_calendar_date, g_max_ty_fin_year_no
 from dwh_foundation.temp_l4l
 ;

select min(this_week_start_date)
into g_min_ty_calendar_date
from dim_calendar
where fin_year_no = g_max_ty_fin_year_no - 3;

 Update DWH_DATAFIX.fnd_rtl_loc_dy_l4L_WL210713 a
 set (like_for_like_ind, 
     like_for_like_adj_ind) = (select  like_for_like_ind, like_for_like_ind
                                  from dwh_foundation.temp_l4l b
                                 where a.location_no = b.location_no
--                                   AND B.ty_calendar_date  <= g_max_ty_calendar_date
                                  AND B.ty_calendar_date  between g_min_ty_calendar_date and g_max_ty_calendar_date
                                   group by like_for_like_ind, like_for_like_ind),
                                   last_updated_date = g_date
where calendar_date between g_min_ty_calendar_date and g_max_ty_calendar_date;
--where calendar_date <= g_max_ty_calendar_date;

  g_rec_cnt := SQL%ROWCOUNT;
  l_text := 'Update from Beginning THIS_fin_year = '||g_rec_cnt;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  DBMS_OUTPUT.PUT_LINE('Update from Beginning THIS_fin_year = '||g_rec_cnt);
 commit;
 
-------------------------
--  update equivalent LAST_FINANCIAL_YEAR to max fin_week of THIS_FINANCIAL_YEAR
------------------------- 
/*select ly_calendar_date, ly_fin_year_no
into g_max_ly_calendar_date, g_ly_fin_year_no
from dim_calendar
where calendar_date = g_max_ty_calendar_date;

 Update dwh_foundation.fnd_rtl_loc_dy_like_4_like a
 set (like_for_like_ind, 
     like_for_like_adj_ind) = (select  like_for_like_ind, like_for_like_ind
                                  from dwh_foundation.temp_l4l b, dim_calendar dc
                                 where b.ty_fin_year_no = dc.fin_year_no
                                   and a.location_no = b.location_no
                                   and a.calendar_date = dc.calendar_date),
                                   last_updated_date = g_date
where calendar_date <= g_max_ly_calendar_date
 ;
  g_rec_cnt := SQL%ROWCOUNT;
  l_text := 'Update from Beginning LAST_fin_year = '||g_rec_cnt;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 commit;
*/

 
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
       
/*
************************************************************************************************************
**
   old code pre 12 june 2013 ***
**
************************************************************************************************************
cursor c_stg_excel_like_4_like is
   select SYS_SOURCE_BATCH_ID,
          SYS_SOURCE_SEQUENCE_NO,
          SYS_MIDDLEWARE_BATCH_ID,
          a.source_data_status_code  c_SOURCE_DATA_STATUS_CODE,
          a.location_no,
          a.fin_year_no              c_fin_year_no,
          a.fin_week_no              c_fin_week_no,
          a.fin_day_no               c_fin_day_no,
          c.calendar_date            c_calendar_date,
          c.ly_calendar_date         c_ly_calendar_date,
          c.ly_fin_year_no           c_ly_fin_year_no,
          c.ly_fin_week_no           c_ly_fin_week_no,
          a.like_for_like_ind        c_like_for_like_ind
   from stg_excel_like_4_like_cpy a,
        dim_calendar c
   where sys_process_code = 'N'
    and c.fin_year_no = a.fin_year_no
     and c.fin_week_no = a.fin_week_no
-- This is commented out as we only receive data for 1 day of the week but
-- need to explode it to all 7 days of the week.
--
  --   and c.fin_day_no = a.fin_day_no
  --
   order by a.location_no,
            c_fin_year_no, c_fin_week_no, c_fin_day_no,
            sys_source_batch_id,sys_source_sequence_no;

   g_rec_in c_stg_excel_like_4_like%rowtype;

--**************************************************************************************************
--                           M A I N     P R O C E S S
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_rtl_loc_dy_like_4_like EX POS STARTED AT '||
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
    

--
-- Following values are used in processing
--
    Select ly_calendar_date, fin_year_no, fin_week_no,
           fin_day_no, ly_fin_year_no, ly_fin_week_no
       into g_ly_calendar_date, g_fin_year_no, g_fin_week_no,
            g_fin_day_no, g_ly_fin_year_no, g_ly_fin_week_no
    from DIM_CALENDAR
    where CALENDAR_DATE = g_date;
    If g_ly_calendar_date is null
    then
       Select min(ly_calendar_date)
          into g_ly_calendar_date
       from DIM_CALENDAR;
       l_text := 'LY BATCH DATE BEING PROCESSED not found , defaulted to - '||g_ly_calendar_date;
    else
       l_text := 'LY BATCH DATE BEING PROCESSED  - '||g_ly_calendar_date;
    end if;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-------------------------------------------------------------------------------
--  DESIGN :
--
--> At the start of each Fin Year (Fin Week 1 Fin Day 1)
--              the values in the L4L Adj column
--              for matching Fin Week and Fin Day of the "Last Year",
--              for the entire ("Last Year") year must be set to zero ("0").
-------------------------------------------------------------------------------
 if g_fin_week_no = 1
 and g_fin_day_no = 1
 then
       update  dwh_foundation.fnd_rtl_loc_dy_like_4_like  rld
               set rld.like_for_like_adj_ind = 0,
                   last_updated_date = g_date
       where rld.calendar_date in(select dc2.calendar_date
                                  from dim_calendar dc2
                                  where dc2.ly_fin_year_no = g_ly_fin_year_no);
        g_recs_zeroised := SQL%ROWCOUNT;
        l_text := 'LY fin_year zeroised at beginning of year : RECORDS UPDATED = '||g_recs_zeroised;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        commit;
  end if;
--**************************************************************************************************
-- M A I N   S E C T I O N
--**************************************************************************************************
  open c_stg_excel_like_4_like;
      loop
          fetch c_stg_excel_like_4_like into g_rec_in;
          exit when c_stg_excel_like_4_like%notfound;

   g_recs_read := g_recs_read+ 1;

   g_hospital      := 'N';
   if not dwh_valid.indicator_field(g_rec_in.c_like_for_like_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;
 --  if not  dwh_valid.fnd_calendar(g_rec_out.calendar_date) then
 --    g_hospital      := 'Y';
 --    g_hospital_text := dwh_constants.vc_date_not_found;
 --    l_text          := dwh_constants.vc_date_not_found||g_rec_out.calendar_date ;
 --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 --  end if;
   if not dwh_valid.fnd_location(g_rec_in.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text          := dwh_constants.vc_location_not_found||g_rec_in.location_no ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

  if g_hospital = 'Y'
  then
      begin
           insert into stg_excel_like_4_like_hsp values
                (g_rec_in.SYS_SOURCE_BATCH_ID
                ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
                ,sysdate
                ,'Y'
                ,'DWH'
                ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
                ,g_hospital_text
                ,g_rec_in.LOCATION_NO
                ,g_rec_in.c_LIKE_FOR_LIKE_IND
                ,g_rec_in.c_SOURCE_DATA_STATUS_CODE
                ,g_rec_in.c_FIN_YEAR_NO
                ,g_rec_in.c_FIN_WEEK_NO
                ,g_rec_in.c_FIN_DAY_NO);
           g_recs_hospital := g_recs_hospital + 1;
      exception
          when dwh_errors.e_insert_error then
               l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
               raise;
          when others then
               l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
               dwh_log.record_error(l_module_name,sqlcode,l_message);
 --              raise;
       end;

    else
-------------------------------------------------------------------------------
--  DESIGN :
--
--> If loading data for the current Fin Year
--       then populate both columns
--       and the L4L Adj column
--          for the same Fin Week, Fin Day "Last Year"
-------------------------------------------------------------------------------
           if g_rec_in.c_fin_year_no = g_fin_year_no
           then
                v_like_for_like_ind     := g_rec_in.c_like_for_like_ind;
                v_like_for_like_adj_ind := g_rec_in.c_like_for_like_ind;
                begin
                     update dwh_foundation.fnd_rtl_loc_dy_like_4_like
                        set like_for_like_adj_ind = g_rec_in.c_like_for_like_ind,
                            last_updated_date = g_date
                      where calendar_date = g_rec_in.c_ly_calendar_date
                        and location_no = g_rec_in.location_no;
                     commit;
                    g_recs_updated := g_recs_updated+ 1;
                exception
                   when no_data_found
                       then
                         l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
                         dwh_log.record_error(l_module_name,sqlcode,l_message);
                end;
           else
-------------------------------------------------------------------------------
--  DESIGN :
--
--> If loading data for a previous Fin Year
--     update only the L4L column
--            for that date
--      and also the L4L Adj column
--             for the same Fin Week, Fin Day "Last Year"
--> For historical records, updates only no inserts for L4L Adj process
-------------------------------------------------------------------------------
           if g_rec_in.c_fin_year_no < g_fin_year_no then
                v_like_for_like_ind := g_rec_in.c_like_for_like_ind;
                begin
                   update dwh_foundation.fnd_rtl_loc_dy_like_4_like
                      set like_for_like_adj_ind = g_rec_in.c_like_for_like_ind,
                          last_updated_date = g_date
                   where calendar_date = g_rec_in.c_ly_calendar_date
                     and location_no = g_rec_in.location_no;
                   commit;
                   g_recs_updated := g_recs_updated+ 1;
           exception
                    when no_data_found
                        then
                            l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
                            dwh_log.record_error(l_module_name,sqlcode,l_message);
                end;
           end if;
           end if;
--**************************************************************************************************
-- Check to see if item is present on table
--      and update/insert accordingly Fin Week, Fin Day "This Year"
--**************************************************************************************************
    g_count := null;

    select count(1)
      into   g_count
     from    dwh_foundation.fnd_rtl_loc_dy_like_4_like
    where  location_no      = g_rec_in.location_no
      and  calendar_date    = g_rec_in.c_calendar_date;

   if g_count = 1 then
       update  dwh_foundation.fnd_rtl_loc_dy_like_4_like
       set    like_for_like_ind            = v_like_for_like_ind,
              like_for_like_adj_ind        = case
                                             when g_rec_in.c_fin_year_no < g_fin_year_no
                                                 then
                                                  g_rec_in.c_like_for_like_ind
                                             else
                                                  like_for_like_adj_ind
                                             end,
              last_updated_date          = g_date
       where  location_no                = g_rec_in.location_no
         and  calendar_date              = g_rec_in.c_calendar_date;
         commit;
        g_recs_updated := g_recs_updated+ 1;
    else
      if g_rec_in.c_fin_year_no = g_fin_year_no then
          insert into  dwh_foundation.fnd_rtl_loc_dy_like_4_like
               values(g_rec_in.LOCATION_NO
                     ,g_rec_in.c_CALENDAR_DATE
                     ,g_rec_in.c_like_for_like_ind
                     , ''
                     ,g_date
                     ,g_rec_in.c_like_for_like_ind);
         commit;
         g_recs_inserted := g_recs_inserted + 1;
      end if;
    end if;
        end if;
     update stg_excel_like_4_like_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = g_rec_in.sys_source_batch_id  and
              sys_source_sequence_no = g_rec_in.sys_source_sequence_no;
     commit;
--    dbms_output.put_line(g_recs_read||' '||g_recs_inserted||' '||g_recs_updated||' '||g_recs_hospital);
   exit when c_stg_excel_like_4_like%notfound;
 end loop;
*/

END WH_FND_CORP_248U_WLL4L;
