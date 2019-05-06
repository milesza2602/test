--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_041U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_041U" 
                                                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2010
--  Author:      Alastair de Wet
--  Purpose:     Create style level  dimentioned by table in the performance layer
--               with dim_item and clearance data ex performance  table.
--  Tables:      Input  - dim_item,fnd_rtl_clearance
--               Output - rtl_lev1_rsp_clearance_crl
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_deleted       integer       :=  0;
g_count              integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            rtl_lev1_rsp_clearance_crl%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_041U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE rtl_lev1_rsp_clearance_crl EX dim_item & clearance';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_lev1_rsp_clearance_crl%rowtype index by binary_integer;
type tbl_array_u is table of rtl_lev1_rsp_clearance_crl%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
-- First cursor below is a shorter version but only works if you can live with spaces between
-- fields. If you require commas then it puts in commas for null values and looks ugly.
-- We needed commas in the strings between sizes and colours.
/*
cursor c_colsize is
with colsize as (

select cl.active_date,
       cl.ruling_rsp,
       di.style_no,
       di.sk1_style_no,
       di.diff_1_code_desc as strng, ' ' as strng1
from   fnd_rtl_clearance cl, dim_item di
where  active_date > trunc(sysdate-100)
and    cl.item_no  = di.item_no
and    di.business_unit_no <> 50
group by di.sk1_style_no,di.style_no,cl.active_date,cl.ruling_rsp,di.diff_1_code_desc
union all
select cl.active_date,
       cl.ruling_rsp,
       di.style_no,
       di.sk1_style_no,
       ' ',di.diff_2_code_desc as strng1
from   fnd_rtl_clearance cl, dim_item di
where  active_date > trunc(sysdate-100)
and    cl.item_no  = di.item_no
and    di.business_unit_no <> 50
group by di.sk1_style_no,di.style_no,cl.active_date,cl.ruling_rsp,di.diff_2_code_desc
)
select style_no,
       active_date,
       ruling_rsp,
       sk1_style_no,
       rtrim (xmlagg (xmlelement (e, strng || ' ')).extract ('//text()'), ',') strng,
       rtrim (xmlagg (xmlelement (e, strng1 || ' ')).extract ('//text()'), ',') strng1
from
       colsize
group by
       style_no,
       active_date,
       ruling_rsp,
       sk1_style_no;
*/
--==============================================================
cursor c_colsize is
with color as (

select cl.active_date,
       cl.ruling_rsp,
       di.style_no,
       di.sk1_style_no,
       di.diff_1_code_desc as strng, ' ' as strng1,
       max(cl.ruling_rsp_local) ruling_rsp_local,
       max(cl.ruling_rsp_opr) ruling_rsp_opr 
       
from   fnd_rtl_clearance cl, dim_item di
where  active_date > trunc(sysdate-215)
and    cl.item_no  = di.item_no
and    di.business_unit_no <> 50
and    cl.markdown_seq_no   = 1
and    cl.clear_status_code = 'A'
--and    di.style_no not in (248468,500228920 )
group by di.sk1_style_no,di.style_no,cl.active_date,cl.ruling_rsp,di.diff_1_code_desc

),
colorswing as
(select sk1_style_no,
       style_no,
       active_date,
       ruling_rsp,
       max(ruling_rsp_local) ruling_rsp_local,
       max(ruling_rsp_opr) ruling_rsp_opr,       
       rtrim (xmlagg (xmlelement (e, strng || ', ')order by strng).extract ('//text()'), ', ') strng,
       rtrim (xmlagg (xmlelement (e, strng1 || ' ')).extract ('//text()'), ',') strng1
from
       color
group by
       sk1_style_no,
       style_no,
       active_date,
       ruling_rsp
       ),

sz as (
select cl.active_date,
       cl.ruling_rsp,
       di.style_no,
       di.sk1_style_no,
       ' ' as strng, trim(di.diff_2_code_desc) as strng1 ,
       max(di.diff_2_display_seq) disp_seq,
       max(cl.ruling_rsp_local) ruling_rsp_local,
       max(cl.ruling_rsp_opr) ruling_rsp_opr      
from   fnd_rtl_clearance cl, dim_item di
where  active_date > trunc(sysdate-215)
and    cl.item_no  = di.item_no
and    di.business_unit_no <> 50
and    cl.markdown_seq_no   = 1
and    cl.clear_status_code = 'A'
--and    di.style_no not in (248468,500228920 )
group by di.sk1_style_no,di.style_no,cl.active_date,cl.ruling_rsp,di.diff_2_code_desc

),
szswing as (
select
       sk1_style_no,
       style_no,
       active_date,
       ruling_rsp,
       max(ruling_rsp_local) ruling_rsp_local,
       max(ruling_rsp_opr) ruling_rsp_opr,     
       rtrim (xmlagg (xmlelement (e, strng || ' ')).extract ('//text()'), ',') strng,
       rtrim (xmlagg (xmlelement (e, strng1 || ', ')order by disp_seq).extract ('//text()'), ', ') strng1
/*
       rtrim (xmlagg (xmlelement (e, strng1 || ',')
       order by decode (strng1,'1','01',
                               '2','02',
                               '3','01',
                               '4','04',
                               '5','05',
                               '6','06',
                               '7','07',
                               '8','08',
                               '9','09',
                               '71','071',
                               '76','076',
                               '81','081',
                               '87','087',
                               '92','092',
                               '97','097',
                                strng1)
                                )

       .extract ('//text()'), ',') strng1
*/
from
       sz
group by
       sk1_style_no,
       style_no,
       active_date,
       ruling_rsp
       ),
colsize as (
select   * from szswing
union all
select * from colorswing)
select style_no,
       active_date,
       ruling_rsp,
       sk1_style_no,
       max(ruling_rsp_local) ruling_rsp_local,
       max(ruling_rsp_opr) ruling_rsp_opr,     
       rtrim (xmlagg (xmlelement (e, strng || ',')).extract ('//text()'), ',') strng,
       rtrim (xmlagg (xmlelement (e, strng1 || ',')).extract ('//text()'), ',') strng1
from
       colsize
group by
       style_no,
       active_date,
       ruling_rsp,
       sk1_style_no;

--===================================================


g_rec_in             c_colsize%rowtype;
-- For input bulk collect --
type stg_array is table of c_colsize%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

    g_rec_out.sk1_style_no                    := g_rec_in.sk1_style_no;
    g_rec_out.ruling_rsp                      := g_rec_in.ruling_rsp;
    g_rec_out.active_date                     := g_rec_in.active_date  ;
    g_rec_out.diff_1_code_desc                := substr(g_rec_in.strng,1,255);
    g_rec_out.diff_2_code_desc                := substr(g_rec_in.strng1,1,255);
    g_rec_out.last_updated_date               := g_date;
    
    g_rec_out.ruling_rsp_local                := g_rec_in.ruling_rsp_local;
    g_rec_out.ruling_rsp_opr                  := g_rec_in.ruling_rsp_opr;
    
    if substr(g_rec_out.diff_1_code_desc,1,1) = ',' then
       g_rec_out.diff_1_code_desc := substr(g_rec_out.diff_1_code_desc,2,250);
    end if;

   exception
      when others then
--     dbms_output.put_line('1 '||g_rec_out.style_no);
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
      insert into rtl_lev1_rsp_clearance_crl values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
--     dbms_output.put_line('2 ');
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).sk1_style_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin





      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;


      a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_insert;


      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;

   exception
      when dwh_errors.e_insert_error then
--     dbms_output.put_line('4 '||g_rec_out.style_no||'  '||g_rec_out.style_colour_no);
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
--     dbms_output.put_line('5 '||g_rec_out.style_no||'  '||g_rec_out.style_colour_no);
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_lev1_rsp_clearance_crl EX dim_item STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Truncate output table before writing new data
--**************************************************************************************************
    l_text := 'Truncate rtl_lev1_rsp_clearance_crl before writing new data - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_performance.rtl_lev1_rsp_clearance_crl';

    l_text := 'Truncate completed - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
    open c_colsize;
    fetch c_colsize bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_colsize bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_colsize;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
    local_bulk_insert;


--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_041u;
