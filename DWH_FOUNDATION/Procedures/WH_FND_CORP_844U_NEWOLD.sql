--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_844U_NEWOLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_844U_NEWOLD" (p_forall_limit in integer,
p_success out boolean
--,
--p_from_loc_no in integer,p_to_loc_no in integer
) as
--**************************************************************************************************
--  Date:        February 2010
--  Author:      M Munnik
--  Purpose:     Load Shipment data - actual receipts - to allocation tracker table.
--               NB> the last 90-days sub-partitions are truncated via a procedure before this one runs.
--               For CHBD only.
--  Tables:      Input  - fnd_alloc_tracker_alloc, fnd_rtl_shipment
--               Output - fnd_alloc_tracker_actl_rcpt
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  12 may 2010 - change num_days calc
--  28 may 2010 - wendy - add cancelled_qty and remove cancel_ind
--  8 june 2010 - wendy - remove carton_status check
--  23 July 2010 - wendy - add p_period_ind and hint to cursor
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
g_recs_inserted      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_date               date;
g_start_date         date;
g_end_date         date;
g_rec_out            fnd_alloc_tracker_actl_rcpt%rowtype;
  g_num_days_to_actl_grn number(14,2);
  g_num_weight_days_to_actl_grn number(14,2);
p_from_loc_no number        :=  0;

p_to_loc_no number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_844U_NEW'||p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOADS Actual Receipts TO ALLOC TRACKER TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_alloc_tracker_actl_rcpt%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_empty_set_i       tbl_array_i;

a_count             integer       := 0;
a_count_i           integer       := 0;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF fnd_alloc_tracker_actl_rcpt EX fnd_rtl_shipment STARTED '||
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
p_from_loc_no := 0;

      IF p_from_loc_no = 0 THEN
        g_start_date := g_date - 30;
      ELSE
        IF p_to_loc_no = 99999 THEN
          g_start_date := g_date - 90;
          g_date       := g_date - 61;
        ELSE
          IF p_from_loc_no > 0 THEN
            g_start_date := g_date - 60;
            g_date       := g_date - 31;
          ELSE
            g_start_date := g_date - 90;
          END IF;
        END IF;
      END IF;

    l_text := 'DATA LOADED FOR PERIOD '||g_start_date||' TO '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
 
 FOR V_CUR in (
    WITH selext AS
            (SELECT
              /*+ full(s) full(a) */
              a.release_date,
              a.alloc_no,
              a.to_loc_no,
              s.actl_rcpt_date actl_grn_date,
              s.du_id,
              a.first_dc_no,
              a.item_no,
              SUM(NVL(s.received_qty,0)) actl_grn_qty,
              SUM(NVL(s.cancelled_qty,0)) cancelled_qty
            FROM fnd_alloc_tracker_alloc a,
              fnd_rtl_shipment s
            WHERE a.item_no        = s.item_no
            AND a.to_loc_no        = s.to_loc_no
            AND (s.actl_rcpt_date IS NOT NULL)
            AND s.received_qty     > 0
            AND a.release_date BETWEEN g_start_date AND g_date
            AND (( a.trunk_ind = 0
            AND a.alloc_no     = s.dist_no)
            OR ( a.trunk_ind   = 1
            AND a.alloc_no     = s.tsf_alloc_no) )
            GROUP BY a.release_date,
              a.alloc_no,
              a.to_loc_no,
              s.actl_rcpt_date,
              s.du_id,
              a.first_dc_no,
              a.item_no
            )
    SELECT release_date,
      alloc_no,
      to_loc_no,
      actl_grn_date,
      du_id,
      first_dc_no,
      item_no,
      actl_grn_qty,
--      (
--      CASE
--        WHEN num_days_to_actl_grn > 0
--             THEN (num_days_to_actl_grn–1)
--        ELSE num_days_to_actl_grn
--      END) 
        nvl(num_days_to_actl_grn,0) num_days_to_actl_grn,
      0 num_weighted_days_to_actl_grn,
      g_date last_updated_date,
      cancelled_qty
    FROM
            (SELECT release_date,
              alloc_no,
              to_loc_no,
              actl_grn_date,
              du_id,
              first_dc_no,
              item_no,
              actl_grn_qty,
              COUNT(DISTINCT dc.calendar_date) num_days_to_actl_grn,
              cancelled_qty
            FROM selext se,
              dim_calendar dc
            WHERE dc.calendar_date BETWEEN se.release_date AND se.actl_grn_date
            AND fin_day_no NOT        IN(6,7)
            AND rsa_public_holiday_ind = 0
            GROUP BY release_date,
              alloc_no,
              to_loc_no,
              actl_grn_date,
              du_id,
              first_dc_no,
              item_no,
              actl_grn_qty,
              cancelled_qty)
  )
  LOOP
  
  if  (V_CUR.num_days_to_actl_grn > 0)
             THEN 
             g_num_days_to_actl_grn := V_CUR.num_days_to_actl_grn - 1;
             end if;
   if (V_CUR.num_days_to_actl_grn is not null) then
      g_num_weight_days_to_actl_grn := V_CUR.num_days_to_actl_grn * V_CUR.actl_grn_qty;
   end if;
--  INSERT /*+ APPEND */ INTO dwh_FOUNDATION.fnd_alloc_tracker_actl_rcpt
  INSERT /*+ APPEND */ INTO DWH_FOUNDATION.TEST_FND_ACTL_RCPT
  VALUES (V_CUR.release_date,
  V_CUR.alloc_no,
  V_CUR.to_loc_no,
  V_CUR.actl_grn_date,
  V_CUR.du_id,
  V_CUR.first_dc_no,
  V_CUR.item_no,
  V_CUR.actl_grn_qty,
  g_num_days_to_actl_grn,
  g_num_weight_days_to_actl_grn,
  V_CUR.last_updated_date,
  V_CUR.cancelled_qty);
    g_recs_count := g_recs_count + to_number(to_char(sql%rowcount));

         if g_recs_count mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read||' inserted='||g_recs_updated ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;
    
    
     commit;
     END LOOP;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read||' '||g_start_date||' TO '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted||' '||g_start_date||' TO '||g_date;
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


END WH_FND_CORP_844U_NEWOLD;
