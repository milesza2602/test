--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_110T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_110T" (p_forall_limit in integer,p_success out boolean, p_from_loc_no in integer,p_to_loc_no in integer
) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID dense sales fact table in the performance layer
--               with input ex RMS Sale table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_rms_sale
--               Output - W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  13 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_fnd_sale             number(14,2)        :=  0;
g_prf_sale             number(14,2)        :=  0;


g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          number       :=  0;
g_recs_inserted      number       :=  0;
g_recs_updated       number       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_rec_out            W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q%rowtype;

g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_wac                rtl_loc_item_dy_rms_price.wac%type             := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_110T_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS DENSE SALES FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_m is table of W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q%rowtype index by binary_integer;

a_tbl_merge         tbl_array_m;
a_empty_set_m       tbl_array_m;

a_count             number       := 0;
a_count_m          number       := 0;
g_process_no number   := 0;
g_from_loc_no number  := 0;
g_to_loc_no   number  := 0;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    
    g_forall_limit := 10000;
    
--    l_text := 'ARRAY LIMIT - '||g_forall_limit;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--    begin
--    execute immediate '  alter session set events ''10046 trace name context forever, level 12''   ';
--    end;

    execute immediate 'alter session enable parallel dml';
    
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '14/AUG/17';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    if p_from_loc_no = 0 then
       g_from_loc_no := 0;
       g_to_loc_no   := 99999;
    end if;
    
    if p_from_loc_no  = 351 then
       g_from_loc_no := 0;
       g_to_loc_no   := 0;
    end if;
    
    if p_from_loc_no  = 491 then
       g_from_loc_no := 0;
       g_to_loc_no   := 0;
    end if;
    
    l_text := 'MERGE STARTING ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  if g_to_loc_no   <> 0 then     
    --MERGE /*+ USE_HASH(rtl_lidrd ,mer_rlidrd)*/ INTO W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q rtl_lidrd
    MERGE /*+ parallel(rtl_lidrd,4) */ INTO W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q rtl_lidrd
    USING
    (
    -- select /*+ parallel(di,4) full(dl) full(dlh) full(dih) parallel(fnd_li,4) full(vr) parallel(fnd_lid,4)  */  
    --select /*+ parallel(fnd_lid,4) full(dl) full(dlh) full(dih) full(vr) index(fnd_li PK_P_RTL_LCTN_ITM) */
    
    select  /*+ full(di) full(dl) full(dlh) full(dih) parallel(vr,4) full(fnd_li) parallel(fnd_li,4) */
              fnd_lid.post_date,
              sales_qty,
              sales,
              sales_cost,
              reg_sales_qty,
              reg_sales,
              reg_sales_cost,
              sdn_in_qty,
              sdn_in_selling,
              sdn_in_cost,
              sales_returns_qty,
              sales_returns,
              sales_returns_cost,
              grn_qty,
              grn_selling,
              grn_cost,
              invoice_adj_qty,
              invoice_adj_selling,
              invoice_adj_cost,
              prom_discount_no,
              di.standard_uom_code,
              di.business_unit_no,
             
             -- If no location/item record is found, the vat region determines default vat rate 
              case when fnd_li.tax_perc is null then
                  case when dl.vat_region_no = 1000 then
                      di.VAT_RATE_PERC
                  else
                      dl.default_tax_region_no_perc
                  end
              else 
                  fnd_li.tax_perc                                  
              end as tax_perc,
             
              di.sk1_department_no,
              di.sk1_item_no,
              dl.chain_no,
              dl.sk1_location_no,
              dl.loc_type,
              decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray) num_units_per_tray,
              nvl(fnd_li.clearance_ind,0) clearance_ind,
              dih.sk2_item_no,
              dlh.sk2_location_no,
              
              sales_returns sales_returns_selling,
              
              -- sales_incl_vat
              case when fnd_li.tax_perc is null then
                  case when dl.vat_region_no = 1000 then
                      round(nvl(sales,0) * (100 + di.VAT_RATE_PERC) / 100,2)
                  else
                      round(nvl(sales,0) * (100 + dl.default_tax_region_no_perc) / 100,2)
                  end
              else 
                  round(nvl(sales,0) * (100 + fnd_li.tax_perc) / 100,2)
              end as sales_incl_vat,
              
              nvl(sales,0) - nvl(sales_cost,0) sales_margin,
              nvl(reg_sales,0) - nvl(reg_sales_cost,0) reg_sales_margin,
              nvl(sales_qty,0) + nvl(sales_returns_qty,0) gross_sales_qty,
              nvl(sales,0) + nvl(sales_returns,0) gross_sales,
              
              -- HERE --
              case when dl.loc_type = 'S' then
                nvl(sdn_in_selling,0) + nvl(grn_selling,0) 
              end as store_deliv_selling,
              
              case when dl.loc_type = 'S' then
                nvl(sdn_in_cost,0) + nvl(grn_cost,0) 
              end as store_deliv_cost,
              
              case when dl.loc_type = 'S' then
                nvl(sdn_in_qty,0)  + nvl(grn_qty,0)
              end as store_deliv_qty,
              
              case when dl.loc_type = 'S' then
                nvl(sdn_in_qty,0) + nvl(grn_qty,0) + nvl(invoice_adj_qty,0)
              end as store_intake_qty ,
              
              case when dl.loc_type = 'S' then
                nvl(sdn_in_selling,0) + nvl(grn_selling,0) + nvl(invoice_adj_selling,0)
              end as store_intake_selling,
              
              case when dl.loc_type = 'S' then
                nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0)
              end as store_intake_cost,
         
              case when dl.loc_type = 'S' then
                (nvl(sdn_in_selling,0) + nvl(grn_selling,0) + nvl(invoice_adj_selling,0)) - (nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0))
              end as store_intake_margin,
      
              --reg_sales_returns_qty
              case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                sales_returns_qty  --else ''
              end as reg_sales_returns_qty,
    
              --reg_sales_returns_selling
              case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                sales_returns --else ''
              end as reg_sales_returns_selling,
              
              --reg_sales_returns_cost
              case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                sales_returns_cost --else ''
              end as reg_sales_returns_cost,
              
    --          --clear_sales_returns_qty
              case when nvl(fnd_li.clearance_ind,0) = 1 then
                sales_returns_qty --else 0
              end as clear_sales_returns_qty,
              
              --clear_sales_returns_selling
              case when nvl(fnd_li.clearance_ind,0) = 1 then
                sales_returns
              end as clear_sales_returns_selling,
              
              --clear_sales_returns_cost
              case when nvl(fnd_li.clearance_ind,0) = 1 then
                sales_returns_cost
              end as clear_sales_returns_cost,
              
              nvl(sales_cost,0) + nvl(sales_returns_cost,0) gross_sales_cost,
              
              --nvl(reg_sales_qty,0) + nvl(reg_sales_returns_qty,0) gross_reg_sales_qty,
              case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                nvl(reg_sales_qty,0) + nvl(sales_returns_qty,0)  
              else 
                nvl(reg_sales_qty,0)
              end as gross_reg_sales_qty,
              
              
              --nvl(reg_sales,0) + nvl(reg_sales_returns_selling,0) gross_reg_sales
              case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                nvl(reg_sales,0) + nvl(sales_returns,0) 
              else 
                nvl(reg_sales,0)
              end as gross_reg_sales,
              
              --gross_reg_sales_cost
              --case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
              case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                nvl(reg_sales_cost,0) + nvl(sales_returns_cost,0) 
              else 
                nvl(reg_sales_cost,0)
              end as gross_reg_sales_cost,
              
              case when di.business_unit_no = 50 then
                round(nvl(sales_qty,0)/ decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray),0)
              end as sales_cases,
              
              case when di.business_unit_no = 50 then
                round(nvl(sdn_in_qty,0)/ decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray),0)
              end as sdn_in_cases,
              
              case when di.business_unit_no = 50 and dl.loc_type = 'S' then
                round(nvl(grn_qty,0) / decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray),0) + 
                round(nvl(sdn_in_qty,0)/ decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray),0) --sdn_in_cases
              end as store_deliv_cases,
              
    --          --g_debtors_commission_perc
    --          case when dl.chain_no = 20 then
    --            vr.debtors_commission_perc
    --          else
    --            0 
    --          end case 
              
              case when dl.chain_no = 20 then
                nvl(sales_cost,0) + round((nvl(sales_cost,0) * nvl(vr.debtors_commission_perc,0) / 100),2)
              --else
              --  nvl(sales_cost,0) + round((nvl(sales_cost,0) * 0 / 100),2)
              end as sales_fr_cost,
              
             case when dl.chain_no = 20 then
                sales
             end as franchise_sales,
              
              case when dl.chain_no = 20 then
                sales - (nvl(sales_cost,0) + round((nvl(sales_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2))
              --else
              --  sales * nvl(sales_cost,0) --+ round((nvl(sales_cost,0) * 0 / 100),2))
              end as franchise_sales_margin,
              
              
             case when dl.chain_no = 20 then
                nvl(reg_sales_cost,0) + round((nvl(reg_sales_cost,0) * nvl(vr.debtors_commission_perc,0) / 100),2)
              --else
              --  nvl(reg_sales_cost,0) --+ round((nvl(reg_sales_cost,0) * 0 / 100),2))
              end as reg_sales_fr_cost,
              
              
              case when dl.chain_no = 20 then
                                    --reg_sales_fr_cost
                nvl(reg_sales,0) - (nvl(reg_sales_cost,0) + round((nvl(reg_sales_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2))
              --else              
              --  nvl(reg_sales,0) - nvl(reg_sales_cost,0) --+ round((nvl(reg_sales_cost,0) * 0 / 100),2))
              end as franchise_reg_sales_margin,
              
              
              case when dl.chain_no = 20 then
                nvl(sdn_in_cost,0) + round((nvl(sdn_in_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2)
              --else
              --  nvl(sdn_in_cost,0) --+ round((nvl(sdn_in_cost,0) * 0 / 100),2)
              end as sdn_in_fr_cost,
              
              
              case when dl.chain_no = 20 then
                case when dl.loc_type = 'S' then
                    --nvl(store_deliv_cost,0) 
                    (nvl(sdn_in_cost,0) + nvl(grn_cost,0)) + round(((nvl(sdn_in_cost,0) + nvl(grn_cost,0)) * nvl(vr.debtors_commission_perc,0)  / 100),2) 
                else
                    (nvl(sdn_in_cost,0) + nvl(grn_cost,0)) --+ round((nvl(store_deliv_cost,0) * vr.debtors_commission_perc / 100),2) 
                end 
              --else 
              --  0
              end as store_deliv_fr_cost,
              
              case when dl.chain_no = 20 then
                nvl(sales_returns_cost,0) + round((nvl(sales_returns_cost,0) * nvl(vr.debtors_commission_perc,0) / 100),2)
              --else
              --  nvl(sales_returns_cost,0) --+ round((nvl(sales_returns_cost,0) * 0 / 100),2)
              end as sales_returns_fr_cost,
              
              
              case when dl.chain_no = 20 then
                case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                    nvl(sales_returns_cost,0) + round((nvl(sales_returns_cost,0) * nvl(vr.debtors_commission_perc,0) / 100),2)
                else
                    0
                end
    --          else
    --            0
              end as reg_sales_returns_fr_cost,
    --          
    --
            
              case when dl.chain_no = 20 then
                 case when nvl(fnd_li.clearance_ind,0) = 1 then    --> for sales_returns_cost
                     nvl(sales_returns_cost,0) + round((nvl(sales_returns_cost,0) * nvl(vr.debtors_commission_perc,0) / 100),2)
                 else 
                     0
                 end
    --          else
    --            0
              end as clear_sales_returns_fr_cost,
              
              
              case when dl.chain_no = 20 then
                                                    -- SALES_FR_COST
                nvl(sales_cost,0) + round((nvl(sales_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2) 
                +                                   -- SALES_RETURNS_FR_COST
                nvl(sales_returns_cost,0) + round((nvl(sales_returns_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2)
              --else
              --  nvl(sales_cost,0) + nvl(sales_returns_cost,0)
              end as gross_sales_fr_cost,
              
              
              case when dl.chain_no = 20 then
                 case when nvl(fnd_li.clearance_ind,0) = 0 and prom_discount_no is null then
                                                    -- REG_SALES_RF_COST
                    nvl(reg_sales_cost,0) + round((nvl(reg_sales_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2) +
                                                   
                                                   -- REG_SALES_RETURNS_FR_COST
                    nvl(sales_returns_cost,0) + round((nvl(sales_returns_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2)
                 else
                    nvl(reg_sales_cost,0) + round((nvl(reg_sales_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2) 
                 end
              --else
              --    nvl(reg_sales_cost,0) + round((nvl(reg_sales_cost,0) * nvl(vr.debtors_commission_perc,0)  / 100),2) 
              end as gross_reg_sales_fr_cost,
              
              case when dl.chain_no = 20 then
                case when dl.loc_type = 'S' then
      --                              -- STORE_INTAKE_COST + (STORE_INTAKE_COST * DEBTORS COMMISSION PERC)
                          (nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0)) + 
                  round(( (nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0)) * nvl(vr.debtors_commission_perc,0) / 100),2)
                end
              end as store_intake_fr_cost,
              
    --          case when dl.chain_no = 20 then
    --            case when dl.loc_type = 'S' then
    ----                              -- STORE_INTAKE_COST + (STORE_INTAKE_COST * DEBTORS COMMISSION PERC)
    --                case when nvl(vr.debtors_commission_perc) is null then
    --                    (nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0)) + 
    --                    round(  (nvl(nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0),0) * vr.debtors_commission_perc / 100),2)
    --                else
    ----                              -- STORE_INTAKE_COST + (STORE_INTAKE_COST * DEBTORS COMMISSION PERC)
    --                    (nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0)) + 
    --                    round(  (nvl(nvl(sdn_in_cost,0) + nvl(grn_cost,0) + nvl(invoice_adj_cost,0),0) * nvl(vr.debtors_commission_perc,0) / 100),2)
    --                end
    --            end
    --          end as store_intake_fr_cost,
              
              '' as ACTL_STORE_RCPT_QTY,
              '' as ACTL_STORE_RCPT_SELLING,
              '' as ACTL_STORE_RCPT_COST,
              '' as ACTL_STORE_RCPT_FR_COST
              
       from   fnd_rtl_loc_item_dy_rms_sale fnd_lid
              join dim_item di on fnd_lid.item_no                      = di.item_no
              
              join dim_location dl on fnd_lid.location_no     = dl.location_no 
                                           
              join dim_item_hist dih on fnd_lid.item_no                = dih.item_no 
                          
              join dim_location_hist dlh on fnd_lid.location_no        = dlh.location_no 
              
              left outer join rtl_location_item fnd_li on di.sk1_item_no      = fnd_li.sk1_item_no  
                                                      and dl.sk1_location_no  = fnd_li.sk1_location_no 
                                                      
              left outer join dwh_performance.rtl_loc_dept_dy vr on dl.sk1_location_no    = vr.sk1_location_no
                                                                and di.sk1_department_no  = vr.sk1_department_no
                                                                and fnd_lid.post_date     = vr.post_date
                                                  
       where  fnd_lid.last_updated_date = g_date 
              and fnd_lid.post_date         between dih.sk2_active_from_date and dih.sk2_active_to_date
              and fnd_lid.post_date         between dlh.sk2_active_from_date and dlh.sk2_active_to_date 
              --and vr.active_from_date       < g_date
                                              
              and ((
              fnd_lid.sales_qty         ||
              fnd_lid.reg_sales_qty     ||
              fnd_lid.sdn_in_qty        ||
              fnd_lid.sales_returns_qty ||
              fnd_lid.grn_qty           ||
              fnd_lid.invoice_adj_qty   ||
              fnd_lid.prom_discount_no) is not null
              )
              --and dl.sk1_location_no = 9102	and di.sk1_item_no = 15898789
              --and di.sk1_item_no = 191562
              
    ) mer_rlidrd
    ON
    (mer_rlidrd.SK1_LOCATION_NO = rtl_lidrd.SK1_LOCATION_NO
    and mer_rlidrd.SK1_ITEM_NO  = rtl_lidrd.SK1_ITEM_NO
    and mer_rlidrd.POST_DATE    = rtl_lidrd.POST_DATE)
    WHEN MATCHED
    THEN
    UPDATE
    SET           sales_qty                       = mer_rlidrd.sales_qty,
                  sales_cases                     = mer_rlidrd.sales_cases,
                  sales                           = mer_rlidrd.sales,
                  sales_incl_vat                  = mer_rlidrd.sales_incl_vat,
                  sales_cost                      = mer_rlidrd.sales_cost,
                  sales_fr_cost                   = mer_rlidrd.sales_fr_cost,
                  sales_margin                    = mer_rlidrd.sales_margin,
                  franchise_sales                 = mer_rlidrd.franchise_sales,
                  franchise_sales_margin          = mer_rlidrd.franchise_sales_margin,
                  reg_sales_qty                   = mer_rlidrd.reg_sales_qty,
                  reg_sales                       = mer_rlidrd.reg_sales,
                  reg_sales_cost                  = mer_rlidrd.reg_sales_cost,
                  reg_sales_fr_cost               = mer_rlidrd.reg_sales_fr_cost,
                  reg_sales_margin                = mer_rlidrd.reg_sales_margin,
                  franchise_reg_sales_margin      = mer_rlidrd.franchise_reg_sales_margin,
                  gross_sales_qty                 = mer_rlidrd.gross_sales_qty,
                  gross_sales                     = mer_rlidrd.gross_sales,
                  gross_sales_cost                = mer_rlidrd.gross_sales_cost,
                  gross_sales_fr_cost             = mer_rlidrd.gross_sales_fr_cost,
                  gross_reg_sales_qty             = mer_rlidrd.gross_reg_sales_qty,
                  gross_reg_sales                 = mer_rlidrd.gross_reg_sales,
                  gross_reg_sales_cost            = mer_rlidrd.gross_reg_sales_cost,
                  gross_reg_sales_fr_cost         = mer_rlidrd.gross_reg_sales_fr_cost,
                  sdn_in_qty                      = mer_rlidrd.sdn_in_qty,
                  sdn_in_selling                  = mer_rlidrd.sdn_in_selling,
                  sdn_in_cost                     = mer_rlidrd.sdn_in_cost,
                  sdn_in_fr_cost                  = mer_rlidrd.sdn_in_fr_cost,
                  sdn_in_cases                    = mer_rlidrd.sdn_in_cases,
                  store_deliv_selling             = mer_rlidrd.store_deliv_selling,
                  store_deliv_cost                = mer_rlidrd.store_deliv_cost,
                  store_deliv_fr_cost             = mer_rlidrd.store_deliv_fr_cost,
                  store_deliv_qty                 = mer_rlidrd.store_deliv_qty,
                  store_deliv_cases               = mer_rlidrd.store_deliv_cases,
                  store_intake_qty                = mer_rlidrd.store_intake_qty,
                  store_intake_selling            = mer_rlidrd.store_intake_selling,
                  store_intake_cost               = mer_rlidrd.store_intake_cost,
                  store_intake_fr_cost            = mer_rlidrd.store_intake_fr_cost,
                  store_intake_margin             = mer_rlidrd.store_intake_margin,
                  sales_returns_qty               = mer_rlidrd.sales_returns_qty,
                  sales_returns_selling           = mer_rlidrd.sales_returns_selling,
                  sales_returns_cost              = mer_rlidrd.sales_returns_cost,
                  sales_returns_fr_cost           = mer_rlidrd.sales_returns_fr_cost,
                  reg_sales_returns_qty           = mer_rlidrd.reg_sales_returns_qty,
                  reg_sales_returns_selling       = mer_rlidrd.reg_sales_returns_selling,
                  reg_sales_returns_cost          = mer_rlidrd.reg_sales_returns_cost,
                  reg_sales_returns_fr_cost       = mer_rlidrd.reg_sales_returns_fr_cost,
                  clear_sales_returns_selling     = mer_rlidrd.clear_sales_returns_selling,
                  clear_sales_returns_cost        = mer_rlidrd.clear_sales_returns_cost,
                  clear_sales_returns_fr_cost     = mer_rlidrd.clear_sales_returns_fr_cost,
                  clear_sales_returns_qty         = mer_rlidrd.clear_sales_returns_qty,
                  last_updated_date               = g_date
    
    WHEN NOT MATCHED
    THEN
    INSERT
    (
                  SK1_LOCATION_NO,
                  SK1_ITEM_NO,
                  POST_DATE,
                  SK2_LOCATION_NO,
                  SK2_ITEM_NO,
                  SALES_QTY,
                  SALES_CASES,
                  SALES,
                  SALES_INCL_VAT,
                  SALES_COST,
                  SALES_FR_COST,
                  SALES_MARGIN,
                  FRANCHISE_SALES,
                  FRANCHISE_SALES_MARGIN,
                  REG_SALES_QTY,
                  REG_SALES,
                  REG_SALES_COST,
                  REG_SALES_FR_COST,
                  REG_SALES_MARGIN,
                  FRANCHISE_REG_SALES_MARGIN,
                  GROSS_SALES_QTY,
                  GROSS_SALES,
                  GROSS_SALES_COST,
                  GROSS_SALES_FR_COST,
                  GROSS_REG_SALES_QTY,
                  GROSS_REG_SALES,
                  GROSS_REG_SALES_COST,
                  GROSS_REG_SALES_FR_COST,
                  SDN_IN_QTY,
                  SDN_IN_SELLING,
                  SDN_IN_COST,
                  SDN_IN_FR_COST,
                  SDN_IN_CASES,
                  ACTL_STORE_RCPT_QTY,
                  ACTL_STORE_RCPT_SELLING,
                  ACTL_STORE_RCPT_COST,
                  ACTL_STORE_RCPT_FR_COST,
                  STORE_DELIV_SELLING,
                  STORE_DELIV_COST,
                  STORE_DELIV_FR_COST,
                  STORE_INTAKE_QTY,
                  STORE_INTAKE_SELLING,
                  STORE_INTAKE_COST,
                  STORE_INTAKE_FR_COST,
                  STORE_INTAKE_MARGIN,
                  SALES_RETURNS_QTY,
                  SALES_RETURNS_SELLING,
                  SALES_RETURNS_COST,
                  SALES_RETURNS_FR_COST,
                  REG_SALES_RETURNS_QTY,
                  REG_SALES_RETURNS_SELLING,
                  REG_SALES_RETURNS_COST,
                  REG_SALES_RETURNS_FR_COST,
                  CLEAR_SALES_RETURNS_SELLING,
                  CLEAR_SALES_RETURNS_COST,
                  CLEAR_SALES_RETURNS_FR_COST,
                  CLEAR_SALES_RETURNS_QTY,
                  LAST_UPDATED_DATE,
                  STORE_DELIV_QTY,
                  STORE_DELIV_CASES
    )
    VALUES
    (
            mer_rlidrd.SK1_LOCATION_NO,
            mer_rlidrd.SK1_ITEM_NO,
            mer_rlidrd.POST_DATE,
            mer_rlidrd.SK2_LOCATION_NO,
            mer_rlidrd.SK2_ITEM_NO,
            mer_rlidrd.SALES_QTY,
            mer_rlidrd.SALES_CASES,
            mer_rlidrd.SALES,
            mer_rlidrd.SALES_INCL_VAT,
            mer_rlidrd.SALES_COST,
            mer_rlidrd.SALES_FR_COST,
            mer_rlidrd.SALES_MARGIN,
            mer_rlidrd.FRANCHISE_SALES,
            mer_rlidrd.FRANCHISE_SALES_MARGIN,
            mer_rlidrd.REG_SALES_QTY,
            mer_rlidrd.REG_SALES,
            mer_rlidrd.REG_SALES_COST,
            mer_rlidrd.REG_SALES_FR_COST,
            mer_rlidrd.REG_SALES_MARGIN,
            mer_rlidrd.FRANCHISE_REG_SALES_MARGIN,
            mer_rlidrd.GROSS_SALES_QTY,
            mer_rlidrd.GROSS_SALES,
            mer_rlidrd.GROSS_SALES_COST,
            mer_rlidrd.GROSS_SALES_FR_COST,
            mer_rlidrd.GROSS_REG_SALES_QTY,
            mer_rlidrd.GROSS_REG_SALES,
            mer_rlidrd.GROSS_REG_SALES_COST,
            mer_rlidrd.GROSS_REG_SALES_FR_COST,
            mer_rlidrd.SDN_IN_QTY,
            mer_rlidrd.SDN_IN_SELLING,
            mer_rlidrd.SDN_IN_COST,
            mer_rlidrd.SDN_IN_FR_COST,
            mer_rlidrd.SDN_IN_CASES,
            mer_rlidrd.ACTL_STORE_RCPT_QTY,
            mer_rlidrd.ACTL_STORE_RCPT_SELLING,
            mer_rlidrd.ACTL_STORE_RCPT_COST,
            mer_rlidrd.ACTL_STORE_RCPT_FR_COST,
            mer_rlidrd.STORE_DELIV_SELLING,
            mer_rlidrd.STORE_DELIV_COST,
            mer_rlidrd.STORE_DELIV_FR_COST,
            mer_rlidrd.STORE_INTAKE_QTY,
            mer_rlidrd.STORE_INTAKE_SELLING,
            mer_rlidrd.STORE_INTAKE_COST,
            mer_rlidrd.STORE_INTAKE_FR_COST,
            mer_rlidrd.STORE_INTAKE_MARGIN,
            mer_rlidrd.SALES_RETURNS_QTY,
            mer_rlidrd.SALES_RETURNS_SELLING,
            mer_rlidrd.SALES_RETURNS_COST,
            mer_rlidrd.SALES_RETURNS_FR_COST,
            mer_rlidrd.REG_SALES_RETURNS_QTY,
            mer_rlidrd.REG_SALES_RETURNS_SELLING,
            mer_rlidrd.REG_SALES_RETURNS_COST,
            mer_rlidrd.REG_SALES_RETURNS_FR_COST,
            mer_rlidrd.CLEAR_SALES_RETURNS_SELLING,
            mer_rlidrd.CLEAR_SALES_RETURNS_COST,
            mer_rlidrd.CLEAR_SALES_RETURNS_FR_COST,
            mer_rlidrd.CLEAR_SALES_RETURNS_QTY,
            g_date,
            mer_rlidrd.STORE_DELIV_QTY,
            mer_rlidrd.STORE_DELIV_CASES
    );

   g_recs_inserted  := g_recs_inserted  + sql%rowcount;    --a_tbl_merge.count;
   g_recs_read      := g_recs_read      + sql%rowcount;
   g_recs_updated   := g_recs_updated   + sql%rowcount;
   
--   g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;
    
        
    --**************************************** CHECK IF LOAD BALANCES ***********************************
    
    select sum(sales) 
    into g_prf_sale
    from W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q
    where post_date = g_date;
    
    select  sum(sales)
    into g_fnd_sale
    from fnd_rtl_loc_item_dy_rms_sale  
    where post_date = g_date;
    
    l_text := ' Foundation sales = '||g_fnd_sale||'   Performance sales = '||g_prf_sale ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    if g_fnd_sale <> g_prf_sale then
       g_prf_sale := g_prf_sale/0;
    end if;   

end if;     

--execute immediate 'alter session set events ''10046 trace name context off'' ';

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
l_text := '- abort-14---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
l_text := '- abort--15--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--16--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
      when others then
l_text := '- abort-17---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
l_text := '- abort-18---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       rollback;
       p_success := false;
l_text := '- abort-19---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--20--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end WH_PRF_CORP_110T;
