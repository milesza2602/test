-- ****** Object: Procedure W7104429.LOAD_STG_DATA Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "LOAD_STG_DATA" 
as

  JobNo user_jobs.job%TYPE;

  l_sql         varchar2(3096);
  l_start       DATE;
  l_end         DATE;
  l_PFX         CHAR(4);
  l_PFX2        CHAR(4);
  l_ins_count   NUMBER;
  l_upd_count   NUMBER;
  l_seconds     NUMBER;
  l_count       NUMBER;
  c             integer;

  g_recs_inserted      integer       :=  0;
  g_recs_updated       integer       :=  0;

BEGIN

-- comment added to test alerts --
    select substr(to_char(today_date,'DAY'),1,2)||to_char(today_date, 'DD')       into l_pfx    from dim_control;
    select substr(to_char(today_date-14,'DAY'),1,2)||to_char(today_date-14, 'DD') into l_pfx2   from dim_control;
--    select substr(to_char(today_date,'DAY'),1,2)||to_char(today_date, 'DD')||'_' into l_pfx from dim_control;
    dbms_output.put_line('STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss')));

    for r in (
                SELECT  /*+ parallel (c,6) */
                        owner,  
                        table_name,
                        l_pfx       pfx,
                        l_pfx2      pfx2
                from    w7104429.ORA_AWX_BTCH_STG_TBLS
                where   owner <> 'DWH_DATAFIX'
              and     table_name  not in ('STG_CUST_CL_INQUIRY')
          and table_name in ('STG_MP_CATALOGUE','STG_JDAFF_PO_PLAN')

                order by  2
             )

    loop
        -- Copy 'CPY' tables to local schema *** in PROD only *** as STG tables - adjust prefix of table name in called proc (eg. 'W7104429.TU_'...)
        dbms_job.submit( JobNo, 'W7104429.INS_STG_DATA(' || ''''||r.owner||'''' || ',' || ''''||r.table_name||'''' || ',' || ''''||r.pfx||'''' || ',' || ''''||r.pfx2||'''' || ');');

        dbms_output.put_line('JobNo : ' || JobNo || '  >>> Table: ' || r.owner || '.' || r.pfx ||  r.table_name);
    end loop;
    commit;

    -- Create a seperate table for the LOB table ...
    l_sql := 'create table w7104429.' || l_pfx || 'STG_CUST_CL_INQUIRY as select sys_source_batch_id, sys_source_sequence_no, sys_load_date, sys_process_code, sys_load_system_name, sys_middleware_batch_id, sys_process_msg, inquiry_no, inq_type_no, inq_type_level_ind, owner_user_no, logged_by_user_no, channel_inbound_no, cl_status_no, interaction_no, inquiry_bus_area, priority, status_central, bus_area_no, owner_grp, transfer_flag, transfer_user, transfer_grp, escal_ind, escal_level, receipt_ackn_required, cust_feedback_required, ''*** NA ***'' inq_details, logged_date, classified_date, receipt_ackn_date, last_prg_upd_date, cust_resolved_date, transfer_date, transfer_acc_date, special_resolved_date, special_ext_period, closed_date, qa_rqd_ind, qa_compl_date, item_no, txt_style_no, product_desc, txt_size, txt_col_fragr, supplier_no, department_no, purchase_date, location_no, item_qty, selling_price, store_refund_given, refund_value, complementary_item_given, complementary_item_value, txt_debit_memo, foods_sell_by_date, foods_prod_batch, foods_mass_size, fo_avail, fo_present, fo_received_date_store, fo_received_date_cl, fo_received_date_tech, svc_staff_member, svc_cia_store_no, cia_incident_date, cia_cust_asstd, cia_claims_impl, cia_asst_given, cia_store_resolved, cia_asst_mgmt, resol_type_lev1, resol_type_lev2, specl_resol_user, cust_resol_user, justified_status, penalty_area, penalty_value, no_corresp_sent, spec_resolved_ind, spec_resolution, cust_resolved_ind, cust_resolution, break_cold_chain, quality_complaint_reason, source_data_status_code, taste_wild_ind, taste_off_ind, taste_rancid_ind, taste_salty_ind, taste_sour_ind, taste_tasteless_ind, taste_chemical_ind, taste_tough_ind, taste_dry_ind, smell_chemical_ind, smell_off_ind, smell_bad_rotten_ind, feel_hard_ind, feel_soft_ind, feel_dry_ind, feel_mushy_ind, look_fatty_ind, look_discoloured_ind, look_separated_ind, look_burnt_ind, look_pale_ind, look_underbaked_raw_ind, look_dry_ind, look_over_ripe_ind, look_under_ripe_ind, packaging_not_sealed_ind, packaging_leaks_ind, packaging_misleading_ind, packaging_blewup_microwave_ind, packaging_lack_of_info_ind, packaging_incorrect_info_ind, packaging_wrong_product_ind, value_poor_value_ind, value_incorrect_price_ind, value_incorrect_promotion_ind, value_too_expensive_ind from dwh_cust_foundation.STG_CUST_CL_INQUIRY_CPY';
    execute immediate l_sql;
    commit;
    
    l_sql := 'update /*+ parallel (s,6) full(s) */  W7104429.' || l_pfx || 'STG_CUST_CL_INQUIRY s set sys_process_code = ''N''';
    execute immediate l_sql;
    commit;
    
    select count(*) into c from sys.dba_tab_cols where owner = 'W7104429' and table_name = l_pfx2||'STG_CUST_CL_INQUIRY';
    if c > 0 then
       l_sql := 'drop table ' || l_pfx2 || 'STG_CUST_CL_INQUIRY';
       EXECUTE IMMEDIATE l_sql; commit;
    end if;
    commit;
    
    dbms_output.put_line('Direct:        >>> Table: W7104429.' || l_pfx || 'STG_CUST_CL_INQUIRY');
    dbms_output.put_line('ENDED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss')));

END "LOAD_STG_DATA"
;
/