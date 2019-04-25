--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_266U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_266U" (p_forall_limit in integer, p_success out boolean)
                         --      p_from_loc_no in integer,  p_to_loc_no in integer)
as
--**************************************************************************************************
--  Date:        Jun 2016
--  Author:      Barry Kirschner 
--  Purpose:     Create daily OnFloor Stock summary table in the performance layer
--               with input ex fact time on offer by day .
--  Tables:      Input  - Rtl_Item_Dy_Too,
--                        Fnd_4f_Loc_Dy_Stocktake,
--                        Fnd_Rtl_Stock_Count_Dtl
--               Output - rtl_cycle_loc_item_dy
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

g_forall_limit       integer       := dwh_constants.vc_forall_limit;
g_recs_read          integer       := 0;
g_recs_updated       integer       := 0;
g_recs_inserted      integer       := 0;
g_recs_hospital      integer       := 0;
g_count              number        := 0;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_max_week           number        := 0;
g_calendar_date      date          := trunc(sysdate);
g_fin_week_no        number        := 0;

g_from_loc_no        integer       := 0;  
g_to_loc_no          integer       := 99999;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_266U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PRF - daily OnFloor Stock fact table';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


begin
    ----------------------------------------
    -- A. Initialize
    ----------------------------------------
    if  p_forall_limit is not null 
    and p_forall_limit > dwh_constants.vc_forall_minimum then
        g_forall_limit := p_forall_limit;
    end if;
 --   if  p_from_loc_no is null then
--        g_from_loc_no := p_from_loc_no;
 --   end if;
 --   if  p_to_loc_no is not null then
 --       g_to_loc_no   := p_to_loc_no; 
 --   end if;
    
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD PRF - daily OnFloor Stock fact table STARTED AT '||  
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    -- Look up batch date from dim_control ...
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    
    ----------------------------------------------------------------------------------------------
    
    -- get current date/week parameters ...
    select  fin_week_no, 
            calendar_date
    into    g_fin_week_no, g_calendar_date
    from    dim_calendar
    where   calendar_date = (select trunc(sysdate) from dual);
    
    l_text := 'CURRENT DATE BEING PROCESSED - '||g_calendar_date||' g_week_no - '||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -----------------------------------
    -- C. PRF Table merge ...
    -----------------------------------/*+ parallel(tgt,4)  */
    merge /*+ parallel(tgt,4)  */ into DWH_PERFORMANCE.rtl_cycle_loc_item_dy tgt 
    USING  (
                 WITH Item_Too AS
                (SELECT
                  /*+ parallel (A,2) */
                  B.Item_No,
                  A.Too_Start_Date,
                  A.Too_End_Date,
                  a.post_date
                FROM Dwh_Performance.Rtl_Item_Dy_Too A,
                  Dim_Item b
--WHERE a.Sk1_Item_No < 10511654 and A.Sk1_Item_No    = B.Sk1_Item_No
                where A.Sk1_Item_No    = B.Sk1_Item_No
                and   a.LAST_UPDATED_DATE = g_date
                ),
                
                onfloor AS
                (SELECT
                  /*+ parallel (B,8) full(B) */
                  A.Cycle_No,
                  B.Location_No,
                  B.Item_No,
                  B.Stocktake_Date,
                  c.post_date,
                  C.Too_Start_Date,
                  C.Too_End_Date,
                  CASE
                    WHEN A.Stocktake_Date BETWEEN C.Too_Start_Date AND C.Too_End_Date
                    THEN 1
                    ELSE 0
                  END too_ind,
                  B.Physical_Count_Qty,
                  NVL(B.Stockroom1_Qty, 0) + NVL(B.Stockroom2_Qty, 0) + NVL(B.Stockroom3_Qty,0) Total_Stockroom_Qty,
                  B.On_Floor_Qty,
                  CASE
                    WHEN B.On_Floor_Qty > 0
                    THEN 1
                    ELSE 0
                  END Sku_On_Display_Ind,
                  CASE
                    WHEN B.On_Floor_Qty                                                               = 0
                    AND NVL(B.Stockroom1_Qty, 0) + NVL(B.Stockroom2_Qty, 0) + NVL(B.Stockroom3_Qty,0) > 0
                    THEN 1
                    ELSE 0
                  END Sku_Off_Display_Ind,
                  CASE
                    WHEN B.On_Floor_Qty                                                               = 0
                    AND NVL(B.Stockroom1_Qty, 0) + NVL(B.Stockroom2_Qty, 0) + NVL(B.Stockroom3_Qty,0) = 0
                    THEN 1
                    ELSE 0
                  END no_stock_in_stockroom_IND
                  --,  sysdate LAST_UPDATED_DATE
                FROM  Fnd_4f_Loc_Dy_Stocktake A,
                      Fnd_Rtl_Stock_Count_Dtl B,
                      item_too c
                WHERE 
                a.cycle_no       = (SELECT MAX(CYCLE_NO) FROM Fnd_4f_Loc_Dy_Stocktake)
                AND   A.Stocktake_Date = B.Stocktake_Date
                and   A.Location_No    = B.Location_No
                
                and   B.Item_No        = C.Item_No 
                and  b.stock_count_no  = (select /*+ full(scd) parallel(scd,8) */  
                                                 max(stock_count_no) 
                                          from   Fnd_Rtl_Stock_Count_Dtl scd 
                                          where  item_no        = b.item_no 
                                          and    location_no    = b.location_no 
                                          and    stocktake_date = b.Stocktake_Date)
                
                ),
                
                Onfloor_too AS
                (SELECT Cycle_No,
                  location_no,
                  Stocktake_Date,
                  COUNT(*) TOO_ITEM_count
                FROM Onfloor
                WHERE Too_Ind = 1     -- 0
                GROUP BY Cycle_No,
                  location_no,
                  Stocktake_Date
                )
              
              SELECT O.*,
                O2.TOO_ITEM_count,
                G_DATE            LAST_UPDATED_DATE
              FROM Onfloor O,
                Onfloor_Too O2
              WHERE O.Cycle_No     = O2.Cycle_No
              AND O.Location_No    = O2.Location_No
              AND O.Stocktake_Date = O2.Stocktake_DatE
          ) src
       on      (tgt.CYCLE_NO        = src.CYCLE_NO
       and      tgt.LOCATION_NO     = src.LOCATION_NO
       and      tgt.ITEM_NO         = src.ITEM_NO
       and      tgt.STOCKTAKE_DATE  = src.STOCKTAKE_DATE
                )

    when matched then
    update set  --STOCKTAKE_DATE	  = src.STOCKTAKE_DATE,
                POST_DATE	        = src.POST_DATE,
                TOO_START_DATE	  = src.TOO_START_DATE,
                TOO_END_DATE	    = src.TOO_END_DATE,
                TOO_IND	          = src.TOO_IND,
                PHYSICAL_COUNT_QTY = src.PHYSICAL_COUNT_QTY,
                TOTAL_STOCKROOM_QTY = src.TOTAL_STOCKROOM_QTY,
                ON_FLOOR_QTY	    = src.ON_FLOOR_QTY,
                SKU_ON_DISPLAY_IND  = src.SKU_ON_DISPLAY_IND,
                SKU_OFF_DISPLAY_IND	= src.SKU_OFF_DISPLAY_IND,
                no_stock_in_stockroom_IND	= src.no_stock_in_stockroom_IND,
                TOO_ITEM_count	        = src.TOO_ITEM_count,
                LAST_UPDATED_DATE	= g_date

    when not matched then
    insert values
              ( src.CYCLE_NO,
                src.LOCATION_NO,
                src.ITEM_NO,
                src.STOCKTAKE_DATE,
                src.POST_DATE,
                src.TOO_START_DATE,
                src.TOO_END_DATE,
                src.TOO_IND,
                src.PHYSICAL_COUNT_QTY,
                src.TOTAL_STOCKROOM_QTY,
                src.ON_FLOOR_QTY,
                src.SKU_ON_DISPLAY_IND,
                src.SKU_OFF_DISPLAY_IND,
                src.no_stock_in_stockroom_IND,
                src.TOO_ITEM_count,
                g_date
              );
    g_recs_updated := + SQL%ROWCOUNT;
    commit;
    
    l_text := 'PRF TABLE rtl_cycle_loc_item_dy Update/Inserted';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_processed||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    
    -----------------------------------
    -- D. Wrap up
    -----------------------------------
    --execute immediate 'truncate table dwh_performance.temp_loc_item_wk_rdf_sysfcst52';
    --commit;
    --l_text := 'TEMP TABLE temp_loc_item_wk_rdf_sysfcst52 TRUNCATED.';
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
       RAISE;
       
end WH_PRF_CORP_266U;
