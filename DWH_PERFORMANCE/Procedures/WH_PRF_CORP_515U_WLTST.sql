--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_515U_WLTST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_515U_WLTST" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      M Munnik
--  Purpose:     Rollup from rtl_loc_item_dy_rms_sparse to rtl_loc_sc_wk_rms_sparse.
--  Tables:      Input  - rtl_loc_item_dy_rms_sparse
--               Output - rtl_loc_sc_wk_rms_sparse
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_sc_wk_rms_sparse%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_fin_year_no         integer;
g_fin_week_no    integer;
g_start_week         integer;
g_start_year         integer;
g_start_date date;
g_END_date date;
G_fin_week_code VARCHAR2(7);
G_SUB NUMBER;

g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_515U_WLTST';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP RMS SPARSE FROM ITEM_DY TO STYLE_COLOUR_WK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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

    l_text := 'ROLLUP OF rtl_loc_sc_wk_rms_sparse EX DAY LEVEL STARTED AT '||
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

g_start_date := '22 AUG 2016';

--for g_sub in 0..10 loop
G_SUB := 0;
    select distinct fin_year_no, fin_week_no, THIS_WEEK_START_DATE, THIS_WEEK_END_DATE, fin_week_code
    into   g_fin_year_no, g_fin_week_no, G_START_DATE, G_END_DATE, G_fin_week_code
    from   dim_calendar
    where  calendar_date = g_start_date - (g_sub * 7);

    l_text := 'ROLLUP RANGE IS:- '||g_fin_year_no||'-'||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_recs_read := 0;
    g_recs_DELETED := 0;

INSERT /*+ APPEND */ INTO dwh_DATAFIX.rtl_loc_sc_wk_rms_sparse_WL
SELECT *  FROM dwh_performance.rtl_loc_sc_wk_rms_sparse A
WHERE EXISTS (select DISTINCT B.sk1_style_colour_no
                        from dim_item B
                        where (B.STYLE_NO IN (9244	,105535	,503585744	,101611163	,304128	,
                                310420	,500201250	,500269814	,500277792	,500358026	,500429928	,500820704	,500836398	,
                                500862796	,500886696	,500886700	,500886718	,500902128	,501027700	,501038280	,501125788	,
                                501125818	,501126298	,501128726	,501248932	,501103286	,501257338	,501261146	,501270470	,
                                501249266	,501286396	,501334710	,501179244	,501333776	,501375074	,501327658	,501050820	,
                                100526924	,100579122	,100655010	,501427832	,101264623	,503577550	,503577534	,101783848	,
                                101783854	,101783889	,503589986	,101383847	,503490790	,503490774	,503362882	,503490782	,
                                503490740	,503490838	,503490870	,503484294	,503484316	,503484324	,503490926	,503490942	,
                                503484332	,503491524	,503491532	,503491540	,503475428	,503491558	,503482080	,503491566	,
                                503469594	,503491718	,503385264	,503491734	,503491742	,503490820	,503490804	,503493002	,
                                503493456	,503590012	,503588870	)
                          OR B.ITEM_NO IN (157636	,42299837	,42299851	,42299868	,3423478491458	,5000394050853	,6001009276438	,
                                  6001009307897	,6005000536102	,6005000675979	,6005000704426	,6005000704440	,6005000964226	,6005000964233	,6005000964264	,
                                  6008000190091	,6009000259313	,6009000341643	,6009000385272	,6009000385357	,6009000445228	,6009000445235	,6009000445242	,
                                  6009000472491	,6009000472583	,6009000472620	,6009000472651	,6009000936382	,6009000936399	,6009000936405	,6009000936412	,
                                  6009000936429	,6009000936436	,6009000936443	,6009000936450	,6009101050178	,6009101050185	,6009101050192	,6009101050208	,
                                  6009101050215	,6009101050222	,6009101050239	,6009101050246	,6009101174645	,6009101174690	,6009101175888	,6009101177585	,
                                  6009101548262	,6009101615261	,6009101626496	,6009101626519	,6009101646791	,6009101646807	,6009101646814	,6009101646821	,
                                  6009101646838	,6009101646845	,6009101646852	,6009101665426	,6009101665433	,6009101665440	,6009101665464	,6009101665471	,
                                  6009101665488	,6009101665495	,6009101665501	,6009101665518	,6009101665525	,6009101665532	,6009101665549	,6009101665556	,
                                  6009101665563	,6009101665570	,6009101665594	,6009101665617	,6009101665624	,6009101665631	,6009101665648	,6009101665655	,
                                  6009101665662	,6009101665679	,6009101665686	,6009101665693	,6009101665709	,6009101665716	,6009101665723	,6009101665730	,
                                  6009101665747	,6009101696574	,6009101736935	,6009101788408	,6009101822645	,6009101836406	,6009101973262	,6009101973279	,
                                  6009101973286	,6009101973293	,6009101973309	,6009101973316	,6009101973323	,6009101993833	,6009101998197	,6009101998203	,
                                  6009101998210	,6009101998227	,6009101998234	,6009101998241	,6009101998258	,6009101998265	,6009101998272	,6009171104511	,
                                  6009171436582	,6009171436599	,6009171499471	,6009171499488	,6009171499495	,6009171499501	,6009171698195	,6009173189684	,
                                  6009173360427	,6009173360700	,9781453006931	,9781631097423		))
                          AND B.sk1_style_colour_no = A.sk1_style_colour_no)
AND A.FIN_YEAR_NO = g_fin_year_no AND A.FIN_WEEK_NO = g_fin_week_no;


    g_recs_read := SQL%ROWCOUNT;
    g_recs_DELETED := SQL%ROWCOUNT;
    commit; 


begin
DELETE FROM dwh_performance.rtl_loc_sc_wk_rms_sparse A
WHERE EXISTS (select DISTINCT B.sk1_style_colour_no
                        from dim_item B
                        where (B.STYLE_NO IN (9244	,105535	,503585744	,101611163	,304128	,
                                310420	,500201250	,500269814	,500277792	,500358026	,500429928	,500820704	,500836398	,
                                500862796	,500886696	,500886700	,500886718	,500902128	,501027700	,501038280	,501125788	,
                                501125818	,501126298	,501128726	,501248932	,501103286	,501257338	,501261146	,501270470	,
                                501249266	,501286396	,501334710	,501179244	,501333776	,501375074	,501327658	,501050820	,
                                100526924	,100579122	,100655010	,501427832	,101264623	,503577550	,503577534	,101783848	,
                                101783854	,101783889	,503589986	,101383847	,503490790	,503490774	,503362882	,503490782	,
                                503490740	,503490838	,503490870	,503484294	,503484316	,503484324	,503490926	,503490942	,
                                503484332	,503491524	,503491532	,503491540	,503475428	,503491558	,503482080	,503491566	,
                                503469594	,503491718	,503385264	,503491734	,503491742	,503490820	,503490804	,503493002	,
                                503493456	,503590012	,503588870	)
                          OR B.ITEM_NO IN (157636	,42299837	,42299851	,42299868	,3423478491458	,5000394050853	,6001009276438	,
                                  6001009307897	,6005000536102	,6005000675979	,6005000704426	,6005000704440	,6005000964226	,6005000964233	,6005000964264	,
                                  6008000190091	,6009000259313	,6009000341643	,6009000385272	,6009000385357	,6009000445228	,6009000445235	,6009000445242	,
                                  6009000472491	,6009000472583	,6009000472620	,6009000472651	,6009000936382	,6009000936399	,6009000936405	,6009000936412	,
                                  6009000936429	,6009000936436	,6009000936443	,6009000936450	,6009101050178	,6009101050185	,6009101050192	,6009101050208	,
                                  6009101050215	,6009101050222	,6009101050239	,6009101050246	,6009101174645	,6009101174690	,6009101175888	,6009101177585	,
                                  6009101548262	,6009101615261	,6009101626496	,6009101626519	,6009101646791	,6009101646807	,6009101646814	,6009101646821	,
                                  6009101646838	,6009101646845	,6009101646852	,6009101665426	,6009101665433	,6009101665440	,6009101665464	,6009101665471	,
                                  6009101665488	,6009101665495	,6009101665501	,6009101665518	,6009101665525	,6009101665532	,6009101665549	,6009101665556	,
                                  6009101665563	,6009101665570	,6009101665594	,6009101665617	,6009101665624	,6009101665631	,6009101665648	,6009101665655	,
                                  6009101665662	,6009101665679	,6009101665686	,6009101665693	,6009101665709	,6009101665716	,6009101665723	,6009101665730	,
                                  6009101665747	,6009101696574	,6009101736935	,6009101788408	,6009101822645	,6009101836406	,6009101973262	,6009101973279	,
                                  6009101973286	,6009101973293	,6009101973309	,6009101973316	,6009101973323	,6009101993833	,6009101998197	,6009101998203	,
                                  6009101998210	,6009101998227	,6009101998234	,6009101998241	,6009101998258	,6009101998265	,6009101998272	,6009171104511	,
                                  6009171436582	,6009171436599	,6009171499471	,6009171499488	,6009171499495	,6009171499501	,6009171698195	,6009173189684	,
                                  6009173360427	,6009173360700	,9781453006931	,9781631097423		))
                          AND B.sk1_style_colour_no = A.sk1_style_colour_no)
AND A.FIN_YEAR_NO = g_fin_year_no AND A.FIN_WEEK_NO = g_fin_week_no;


    g_recs_read := SQL%ROWCOUNT;
    g_recs_DELETED := SQL%ROWCOUNT;
    commit; 



    l_text := 'DELETED RECS = '||G_RECS_DELETED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
exception
         when no_data_found then

    l_text := 'DELETED RECS = 0';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end;

    l_text := 'Delete completed, Insert starting';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
BEGIN
INSERT  /*+ APPEND */ INTO  dwh_performance.rtl_loc_sc_wk_rms_sparse rtl_lswr
with 
        selitem as (select DISTINCT SK1_ITEM_NO, sk1_style_colour_no, style_colour_no, sk1_style_no, style_no
                        from dim_item
                        where STYLE_NO IN (9244	,105535	,503585744	,101611163	,304128	,
                                310420	,500201250	,500269814	,500277792	,500358026	,500429928	,500820704	,500836398	,
                                500862796	,500886696	,500886700	,500886718	,500902128	,501027700	,501038280	,501125788	,
                                501125818	,501126298	,501128726	,501248932	,501103286	,501257338	,501261146	,501270470	,
                                501249266	,501286396	,501334710	,501179244	,501333776	,501375074	,501327658	,501050820	,
                                100526924	,100579122	,100655010	,501427832	,101264623	,503577550	,503577534	,101783848	,
                                101783854	,101783889	,503589986	,101383847	,503490790	,503490774	,503362882	,503490782	,
                                503490740	,503490838	,503490870	,503484294	,503484316	,503484324	,503490926	,503490942	,
                                503484332	,503491524	,503491532	,503491540	,503475428	,503491558	,503482080	,503491566	,
                                503469594	,503491718	,503385264	,503491734	,503491742	,503490820	,503490804	,503493002	,
                                503493456	,503590012	,503588870	)
                          OR ITEM_NO IN (157636	,42299837	,42299851	,42299868	,3423478491458	,5000394050853	,6001009276438	,
                                  6001009307897	,6005000536102	,6005000675979	,6005000704426	,6005000704440	,6005000964226	,6005000964233	,6005000964264	,
                                  6008000190091	,6009000259313	,6009000341643	,6009000385272	,6009000385357	,6009000445228	,6009000445235	,6009000445242	,
                                  6009000472491	,6009000472583	,6009000472620	,6009000472651	,6009000936382	,6009000936399	,6009000936405	,6009000936412	,
                                  6009000936429	,6009000936436	,6009000936443	,6009000936450	,6009101050178	,6009101050185	,6009101050192	,6009101050208	,
                                  6009101050215	,6009101050222	,6009101050239	,6009101050246	,6009101174645	,6009101174690	,6009101175888	,6009101177585	,
                                  6009101548262	,6009101615261	,6009101626496	,6009101626519	,6009101646791	,6009101646807	,6009101646814	,6009101646821	,
                                  6009101646838	,6009101646845	,6009101646852	,6009101665426	,6009101665433	,6009101665440	,6009101665464	,6009101665471	,
                                  6009101665488	,6009101665495	,6009101665501	,6009101665518	,6009101665525	,6009101665532	,6009101665549	,6009101665556	,
                                  6009101665563	,6009101665570	,6009101665594	,6009101665617	,6009101665624	,6009101665631	,6009101665648	,6009101665655	,
                                  6009101665662	,6009101665679	,6009101665686	,6009101665693	,6009101665709	,6009101665716	,6009101665723	,6009101665730	,
                                  6009101665747	,6009101696574	,6009101736935	,6009101788408	,6009101822645	,6009101836406	,6009101973262	,6009101973279	,
                                  6009101973286	,6009101973293	,6009101973309	,6009101973316	,6009101973323	,6009101993833	,6009101998197	,6009101998203	,
                                  6009101998210	,6009101998227	,6009101998234	,6009101998241	,6009101998258	,6009101998265	,6009101998272	,6009171104511	,
                                  6009171436582	,6009171436599	,6009171499471	,6009171499488	,6009171499495	,6009171499501	,6009171698195	,6009173189684	,
                                  6009173360427	,6009173360700	,9781453006931	,9781631097423		)
)
select   /*+ USE_HASH (lid, di, dc) PARALLEL (lid, 2) */
            lid.sk1_location_no as sk1_location_no,
            di.sk1_style_colour_no as sk1_style_colour_no,
            G_fin_year_no fin_year_no,
            G_fin_week_no fin_week_no,
            G_fin_week_code fin_week_code,
            G_start_date this_week_start_date,
            max(lid.sk2_location_no) sk2_location_no,
            sum(lid.prom_sales_qty) prom_sales_qty,
            sum(lid.prom_sales) prom_sales,
            sum(lid.prom_sales_cost) prom_sales_cost,
            sum(lid.prom_sales_fr_cost) prom_sales_fr_cost,
            sum(lid.prom_sales_margin) prom_sales_margin,
            sum(lid.franchise_prom_sales) franchise_prom_sales,
            sum(lid.franchise_prom_sales_margin) franchise_prom_sales_margin,
            sum(lid.prom_discount_no) prom_discount_no,
            sum(lid.ho_prom_discount_amt) ho_prom_discount_amt,
            sum(lid.ho_prom_discount_qty) ho_prom_discount_qty,
            sum(lid.st_prom_discount_amt) st_prom_discount_amt,
            sum(lid.st_prom_discount_qty) st_prom_discount_qty,
            sum(lid.clear_sales_qty) clear_sales_qty,
            sum(lid.clear_sales) clear_sales,
            sum(lid.clear_sales_cost) clear_sales_cost,
            sum(lid.clear_sales_fr_cost) clear_sales_fr_cost,
            sum(lid.clear_sales_margin) clear_sales_margin,
            sum(lid.franchise_clear_sales) franchise_clear_sales,
            sum(lid.franchise_clear_sales_margin) franchise_clear_sales_margin,
            sum(lid.waste_qty) waste_qty,
            sum(lid.waste_selling) waste_selling,
            sum(lid.waste_cost) waste_cost,
            sum(lid.waste_fr_cost) waste_fr_cost,
            sum(lid.shrink_qty) shrink_qty,
            sum(lid.shrink_selling) shrink_selling,
            sum(lid.shrink_cost) shrink_cost,
            sum(lid.shrink_fr_cost) shrink_fr_cost,
            sum(lid.gain_qty) gain_qty,
            sum(lid.gain_selling) gain_selling,
            sum(lid.gain_cost) gain_cost,
            sum(lid.gain_fr_cost) gain_fr_cost,
            sum(lid.grn_qty) grn_qty,
            sum(lid.grn_cases) grn_cases,
            sum(lid.grn_selling) grn_selling,
            sum(lid.grn_cost) grn_cost,
            sum(lid.grn_fr_cost) grn_fr_cost,
            sum(lid.grn_margin) grn_margin,
            sum(lid.shrinkage_qty) shrinkage_qty,
            sum(lid.shrinkage_selling) shrinkage_selling,
            sum(lid.shrinkage_cost) shrinkage_cost,
            sum(lid.shrinkage_fr_cost) shrinkage_fr_cost,
            sum(lid.abs_shrinkage_qty) abs_shrinkage_qty,
            sum(lid.abs_shrinkage_selling) abs_shrinkage_selling,
            sum(lid.abs_shrinkage_cost) abs_shrinkage_cost,
            sum(lid.abs_shrinkage_fr_cost) abs_shrinkage_fr_cost,
            sum(lid.claim_qty) claim_qty,
            sum(lid.claim_selling) claim_selling,
            sum(lid.claim_cost) claim_cost,
            sum(lid.claim_fr_cost) claim_fr_cost,
            sum(lid.self_supply_qty) self_supply_qty,
            sum(lid.self_supply_selling) self_supply_selling,
            sum(lid.self_supply_cost) self_supply_cost,
            sum(lid.self_supply_fr_cost) self_supply_fr_cost,
            sum(lid.wac_adj_amt) wac_adj_amt,
            sum(lid.invoice_adj_qty) invoice_adj_qty,
            sum(lid.invoice_adj_selling) invoice_adj_selling,
            sum(lid.invoice_adj_cost) invoice_adj_cost,
            sum(lid.rndm_mass_pos_var) rndm_mass_pos_var,
            sum(lid.mkup_selling) mkup_selling,
            sum(lid.mkup_cancel_selling) mkup_cancel_selling,
            sum(lid.mkdn_selling) mkdn_selling,
            sum(lid.mkdn_cancel_selling) mkdn_cancel_selling,
            sum(lid.prom_mkdn_qty) prom_mkdn_qty,
            sum(lid.prom_mkdn_selling) prom_mkdn_selling,
            sum(lid.clear_mkdn_selling) clear_mkdn_selling,
            sum(lid.mkdn_sales_qty) mkdn_sales_qty,
            sum(lid.mkdn_sales) mkdn_sales,
            sum(lid.mkdn_sales_cost) mkdn_sales_cost,
            sum(lid.net_mkdn) net_mkdn,
            sum(lid.rtv_qty) rtv_qty,
            sum(lid.rtv_cases) rtv_cases,
            sum(lid.rtv_selling) rtv_selling,
            sum(lid.rtv_cost) rtv_cost,
            sum(lid.rtv_fr_cost) rtv_fr_cost,
            sum(lid.sdn_out_qty) sdn_out_qty,
            sum(lid.sdn_out_selling) sdn_out_selling,
            sum(lid.sdn_out_cost) sdn_out_cost,
            sum(lid.sdn_out_fr_cost) sdn_out_fr_cost,
            sum(lid.sdn_out_cases) sdn_out_cases,
            sum(lid.ibt_in_qty) ibt_in_qty,
            sum(lid.ibt_in_selling) ibt_in_selling,
            sum(lid.ibt_in_cost) ibt_in_cost,
            sum(lid.ibt_in_fr_cost) ibt_in_fr_cost,
            sum(lid.ibt_out_qty) ibt_out_qty,
            sum(lid.ibt_out_selling) ibt_out_selling,
            sum(lid.ibt_out_cost) ibt_out_cost,
            sum(lid.ibt_out_fr_cost) ibt_out_fr_cost,
            sum(lid.net_ibt_qty) net_ibt_qty,
            sum(lid.net_ibt_selling) net_ibt_selling,
            sum(lid.shrink_excl_some_dept_cost) shrink_excl_some_dept_cost,
            sum(lid.gain_excl_some_dept_cost) gain_excl_some_dept_cost,
            sum(lid.net_waste_qty) net_waste_qty,
            sum(lid.trunked_qty) trunked_qty,
            sum(lid.trunked_cases) trunked_cases,
            sum(lid.trunked_selling) trunked_selling,
            sum(lid.trunked_cost) trunked_cost,
            sum(lid.dc_delivered_qty) dc_delivered_qty,
            sum(lid.dc_delivered_cases) dc_delivered_cases,
            sum(lid.dc_delivered_selling) dc_delivered_selling,
            sum(lid.dc_delivered_cost) dc_delivered_cost,
            sum(lid.net_inv_adj_qty) net_inv_adj_qty,
            sum(lid.net_inv_adj_selling) net_inv_adj_selling,
            sum(lid.net_inv_adj_cost) net_inv_adj_cost,
            sum(lid.net_inv_adj_fr_cost) net_inv_adj_fr_cost,
            sum(lid.ch_alloc_qty) ch_alloc_qty,
            sum(lid.ch_alloc_selling) ch_alloc_selling,
            g_date as last_updated_date
   from     rtl_loc_item_dy_rms_sparse lid, SELITEM di 
   WHERE lid.sk1_item_no = di.sk1_item_no
   AND POST_DATE BETWEEN G_START_DATE AND G_END_DATE
   group by lid.sk1_location_no,
            di.sk1_style_colour_no,
            G_fin_year_no,
            G_fin_week_no,
            G_fin_week_code,
            G_start_date;


    g_recs_read :=  SQL%ROWCOUNT;
    g_recs_inserted := SQL%ROWCOUNT;
    commit; 
    l_text := 'INSERTED RECS = '||G_RECS_INSERTED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
exception
         when no_data_found then
           l_text := 'INSERTED RECS = 0';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end;

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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_515u_WLTST;
