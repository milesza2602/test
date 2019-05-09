--------------------------------------------------------
--  DDL for Procedure WH_PRF_BRTH_001X
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_BRTH_001X" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2010
--  Author:      Wendy Lyttle
--  Purpose:     BRIDGETHORN EXTRACT
--               Extract catalog data for :
--               last 4 weeks (this_week_start_date between today - 4 weeks and today)
--               stores only (loc_type = 'S')
--               foods only (business_unit_no = 50)
--               any area except 'NON-CUST CORPORATE' (area_no <> 9978)
--               certain foods departments (department_no in(12 ,15 ,16 ,
--                                                           22 ,23 ,32 ,34 ,
--                                                           37 ,40 ,41 ,42 ,
--                                                           43 ,44 ,45 ,53 ,
--                                                           59 ,66 ,73 ,87 ,
--                                                           88 ,93 ,95 ,97 ,
--                                                           99 )
--               For product_status_1_code, take the value for the max_no_of_locations for the week
--  Tables:      Input  - rtl_loc_item_wk_catalog
--                        dim_item
--                        dim_location
--               Output - temp_rtl_area_item_wk_catalog
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_fin_day_no             number        :=  0;
g_rec_out            temp_rtl_area_item_wk_catalog%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_001X';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'BRIDGETHORN ROLLUP TO TEMP_CATALOG';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
    l_text := 'ROLLUP OF temp_rtl_loc_item_wk_catalog EX WEEK LEVEL STARTED '||
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
    --- testing
    --g_date := '31 may 2010';
    ---- testing
    
    SELECT fin_day_no
    into g_fin_day_no
    FROM dim_calendar
    WHERE calendar_date = g_date ;
    IF g_fin_day_no <> 7 THEN
    SELECT this_week_start_date-28,
      this_week_start_date     -1
    INTO g_start_date,
      g_end_date
    FROM dim_calendar
    WHERE calendar_date = g_date;
    ELSE
      SELECT this_week_start_date-21,
        g_date
     INTO g_start_date,
       g_end_date
      FROM dim_calendar
      WHERE calendar_date = g_date;
    END IF;
    
    
    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate ('truncate table dwh_performance.temp_product_status_1_code');
    commit;
    l_text := 'TRUNCATED table dwh_performance.temp_product_status_1_code';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
FOR v_cur IN
(     WITH selcat AS
  (SELECT fin_year_no ,
    fin_week_no ,
    item_no ,
    MAX(CNT_LOC) maxloc
  FROM
    (SELECT dc.fin_year_no ,
      dc.fin_week_no ,
      di.item_no ,
      rzi.product_status_1_code product_status_1_code,
      COUNT(DISTINCT RZI.SK1_LOCATION_NO) CNT_LOC
    FROM dwh_performance.dim_item DI,
      dwh_performance.dim_location DL,
      dwh_performance.dim_calendar Dc,
      dwh_performance.rtl_loc_item_wk_catalog rzi
    WHERE dc.this_week_start_date BETWEEN g_start_date AND g_end_date
    AND dc.fin_day_no       = 7
    AND rzi.sk1_location_no = dl.sk1_location_no
    AND dc.fin_year_no      = rzi.fin_year_no
    AND dc.fin_week_no      = rzi.fin_week_no
    AND di.sk1_item_no      = rzi.sk1_item_no
    AND dl.area_no          = 9951
    AND di.business_unit_no = 50
    AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99 )
      --        AND DC.FIN_YEAR_NO = 2011 AND DC.FIN_WEEK_NO = 11 AND DI.ITEM_NO = 116
    GROUP BY dc.fin_year_no ,
      dc.fin_week_no ,
      di.item_no ,
      rzi.product_status_1_code
    )
  GROUP BY fin_year_no, fin_week_no, item_no
  ) 
  SELECT a.fin_year_no FIN_YEAR_NO,
    a.fin_week_no FIN_WEEK_NO,
    a.SK1_item_no SK1_ITEM_NO,
    MAX(a.product_status_1_code) product_status_1_code
  FROM
    (SELECT dc.fin_year_no  FIN_YEAR_NO,
      dc.fin_week_no  FIN_WEEK_NO,
      di.SK1_item_no  SK1_ITEM_NO,
      DI.ITEM_NO ITEM_NO,
      rzi.product_status_1_code product_status_1_code,
      COUNT(DISTINCT RZI.SK1_LOCATION_NO) CNT_LOC
    FROM dwh_performance.dim_item DI,
      dwh_performance.dim_location DL,
      dwh_performance.dim_calendar Dc,
      dwh_performance.rtl_loc_item_wk_catalog rzi
    WHERE dc.this_week_start_date BETWEEN g_start_date AND g_end_date
    AND dc.fin_day_no       = 7
    AND rzi.sk1_location_no = dl.sk1_location_no
    AND dc.fin_year_no      = rzi.fin_year_no
    AND dc.fin_week_no      = rzi.fin_week_no
    AND di.sk1_item_no      = rzi.sk1_item_no
    AND dl.area_no          = 9951
    AND di.business_unit_no = 50
    AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99 )
      --         AND DC.FIN_YEAR_NO = 2011 AND DC.FIN_WEEK_NO = 11 AND DI.ITEM_NO = 116
    group by dc.fin_year_no, dc.fin_week_no, di.SK1_item_no, DI.ITEM_NO, rzi.product_status_1_code
    ) a,
    selcat b
  WHERE a.fin_year_no = b.fin_year_no
  AND a.fin_week_no   = b.fin_week_no
  AND a.item_no       = b.item_no
  AND a.cnt_loc       = b.maxloc
    group by a.fin_year_no, a.fin_week_no, a.SK1_item_no 
)
LOOP
  INSERT
    /*+ APPEND */
  INTO DWH_PERFORMANCE.TEMP_product_status_1_code  VALUES
    (V_CUR.fin_year_no,
    V_CUR.fin_week_no,
    V_CUR.SK1_item_no,
    V_CUR.product_status_1_code);

   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

commit;
end loop;

----
    execute immediate ('truncate table dwh_performance.temp_rtl_area_item_wk_catalog');
    commit;
    l_text := 'TRUNCATED table dwh_performance.temp_rtl_area_item_wk_catalog';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      INSERT INTO dwh_performance.temp_rtl_area_item_wk_catalog
            SELECT dlc.area_no ,
          b.item_no,
          b.fin_year_no ,
          b.fin_week_no ,
          b.st_store_type ,
          b.this_week_start_date ,
           b.product_status_1_code ,
          SUM(NVL(b.sumcat,0)) sumcatx
      from 
          dwh_performance.dim_location dlc,
         (
         SELECT Z.location_no ,
          Z.ITEM_NO,
          Z.fin_year_no ,
          Z.fin_week_no ,
          Z.st_store_type ,
          Z.this_week_start_date ,
          Z.product_status_1_code ,
          Z.sumcat
          FROM
         (SELECT dl.location_no location_no ,
          (case when fpid.pack_item_no is null then di.item_no else fpid.pack_item_no end) ITEM_NO,
          rai.fin_year_no fin_year_no ,
          rai.fin_week_no fin_week_no ,
          dl.st_store_type st_store_type ,
          rai.this_week_start_date this_week_start_date ,
          sc.product_status_1_code product_status_1_code ,
          NVL(rai.this_wk_catalog_ind,0) sumcat
        FROM dwh_performance.rtl_loc_item_wk_catalog rai
        LEFT OUTER JOIN dwh_performance.dim_item DI
            ON di.sk1_item_no = rai.sk1_item_no
        LEFT OUTER JOIN dwh_performance.dim_location DL
            ON dl.sk1_location_no = rai.sk1_location_no
        LEFT OUTER JOIN dwh_performance.dim_calendar dc
            ON dc.fin_year_no  = rai.fin_year_no
            AND dc.fin_week_no = rai.fin_week_no
        LEFT OUTER JOIN DWH_PERFORMANCE.TEMP_product_status_1_code sc
            ON sc.fin_year_no  = rai.fin_year_no
            AND sc.fin_week_no = rai.fin_week_no
            AND sc.SK1_ITEM_no = rai.SK1_ITEM_no
        left outer join dwh_foundation.fnd_pack_item_detail fpid
           on fpid.item_no = di.item_no
        WHERE dc.this_week_start_date BETWEEN G_start_date AND G_end_date
        AND dc.fin_day_no       = 7
        AND dl.loc_type         = 'S'
        AND dl.area_no         <> 9978
        AND di.business_unit_no = 50
        AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99 )
 --                 and dc.this_week_start_date = '13/sep/2010'
 --       and di.item_no in (20204648,6001009013187,6001009003355)
    --    and st_store_type = 'WW'
        ) Z
          GROUP BY 
          Z.location_no ,
          Z.ITEM_NO,
          Z.fin_year_no ,
          Z.fin_week_no ,
          Z.st_store_type ,
          Z.this_week_start_date ,
          Z.product_status_1_code ,
          Z.sumcat
          ) b
        where dlc.location_no = b.location_no 
        group by dlc.area_no, b.item_no, b.fin_year_no, b.fin_week_no, b.st_store_type, b.this_week_start_date, b.product_status_1_code
;

   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

END WH_PRF_BRTH_001X;