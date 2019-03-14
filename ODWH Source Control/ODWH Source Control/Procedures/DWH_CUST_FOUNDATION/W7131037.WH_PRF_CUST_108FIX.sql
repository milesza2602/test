-- ****** Object: Procedure W7131037.WH_PRF_CUST_108FIX Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_108FIX" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        AUGUST 2011
--  Author:      Alastair de Wet
--  Purpose:     Create cust_basket performance fact table in the performance layer
--               with added value ex foundation layer fnd_cust_basket.
--  Tables:      Input  - fnd_cust_basket
--               Output - cust_basket
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_comm          integer       :=  0;

g_customer_no        number(20,0);
g_tabl_customer_no   number(20,0);
g_pci                number(20,0);

g_tender_no          number(20,0);
g_ww_swipe           number(20,0);
g_ext_swipe          number(20,0);
g_online_order_no    cust_basket.ww_online_order_no%type;

g_tender_no_rank     number(2,0);
g_ww_swipe_rank      number(2,0);
g_ext_swipe_rank     number(2,0);

g_location_no        cust_basket.location_no%type;
g_till_no            cust_basket.till_no%type;
g_tran_no            cust_basket.tran_no%type;
g_tran_date          cust_basket.tran_date%type;




g_found              boolean;
g_count              integer       :=  0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_108U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE BASKET TRANSACTIONS WITH PRIMARY CUSTOMER IDENTIFIER';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

--FULL(cb)

cursor c_prf_cust_basket is
   select /*+  parallel (cb,4) */ location_no,till_no,tran_no,tran_date,loyalty_ww_swipe_no,loyalty_ext_swipe_no,ww_online_order_no
   from   cust_basket cb
   where  cb.last_updated_date     =  '17 Sep 2015'
   and    cb.tran_date             between '10 Sep 2015' and '17 Sep 2015'
--   and   (cb.primary_customer_identifier = 0 or cb.primary_customer_identifier is null)
   and    cb.tran_type in('R','S','V','Q','P','N')
   ;


--   and    cb.ww_online_order_no is null and     -- Online transactions
--          cb.till_no <> 999                     -- Online transactions


--**************************************************************************************************
-- Look for a customer no for online transactions
--**************************************************************************************************
procedure lookup_online_customer_no as
begin

   g_customer_no := null;

   begin
      with atg as
      ( select atg_customer_no
        from   cust_basket_aux
        where  till_no         = g_till_no and
               tran_no         = g_tran_no and
               tran_date       = g_tran_date and
               location_no     = g_location_no and
               atg_customer_no is not null)
      select customer_no
      into   g_customer_no
      from   fnd_customer_online_product, atg
      where  product_no      = atg.atg_customer_no and
             dummy_ind <> 1;
      exception
            when no_data_found then
            g_customer_no := null;
    end;

/*
    begin
      select customer_no
      into   g_customer_no
      from   cust_basket_aux
      where  till_no         = g_till_no and
             tran_no         = g_tran_no and
             tran_date       = g_tran_date and
             location_no     = g_location_no and
             atg_customer_no is not null;
      exception
            when no_data_found then
            g_customer_no := null;
    end;
*/

   exception
      when others then
       l_message := 'LOOKUP CUSTOMER NO - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end lookup_online_customer_no;



--**************************************************************************************************
-- Look for a customer number on the master
--**************************************************************************************************
procedure lookup_customer_no as
begin

   g_customer_no := null;

   if g_tender_no is not null then
    begin
      select customer_no
      into   g_customer_no
      from   fnd_customer_card
      where  card_no   =  g_tender_no and
             dummy_ind <> 1;
      exception
            when no_data_found then
            g_customer_no := null;
    end;
   end if;

   if g_ww_swipe is not null and
      g_customer_no is null  then
    begin
      select customer_no
      into   g_customer_no
      from   fnd_customer_card
      where  card_no = g_ww_swipe  and
             dummy_ind <> 1;
      exception
            when no_data_found then
            g_customer_no := null;
    end;
   end if;

   if g_ext_swipe is not null and
      g_customer_no is null  then
    begin
      select customer_no
      into   g_customer_no
      from   fnd_customer_card
      where  card_no = g_ext_swipe and
             dummy_ind <> 1;
      exception
            when no_data_found then
            g_customer_no := null;
    end;
   end if;

   exception
      when others then
       l_message := 'LOOKUP CUSTOMER NO - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end lookup_customer_no;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure evaluate_pci_priority as
begin

   g_ww_swipe_rank := 9;

   if g_ww_swipe is not null then

        if substr(g_ww_swipe,1,8) in(60078501,60078502,60078503,60078504,
             60078506,60078507,60078509,60078521,60078580) then
             g_ww_swipe_rank := 1;  -- WW store card
        end if;
        if substr(g_ww_swipe,1,6) in(400154,410374,410375,416454,416455) then
             g_ww_swipe_rank := 2;  -- WW credit card
        end if;
        if substr(g_ww_swipe,1,8) in(60078528,60078529,60078530,60078518) then
             g_ww_swipe_rank := 3;  -- WW wod card
        end if;
        if substr(g_ww_swipe,1,4) in(5900) then
             g_ww_swipe_rank := 4;  -- My School card
        end if;
        if substr(g_ww_swipe,1,6) not in(600785,400154,410374,410375,416454,416455,900154,910374,910375)
             and substr(g_ww_swipe,1,4)<>5900 then
             g_ww_swipe_rank := 5;  -- Alien Card
        end if;
        if substr(g_ww_swipe,1,7) in(6007854,6007855) then
             g_ww_swipe_rank := 6;  -- WW Gift card
        end if;
   end if;

   g_ext_swipe_rank := 9;
   if g_ext_swipe is not null then

        if substr(g_ext_swipe,1,8) in(60078501,60078502,60078503,60078504,
             60078506,60078507,60078509,60078521,60078580) then
             g_ext_swipe_rank := 1;  -- WW store card
        end if;
        if substr(g_ext_swipe,1,6) in(400154,410374,410375,416454,416455) then
             g_ext_swipe_rank := 2;  -- WW credit card
        end if;
        if substr(g_ext_swipe,1,8) in(60078528,60078529,60078530,60078518) then
             g_ext_swipe_rank := 3;  -- WW wod card
        end if;
        if substr(g_ext_swipe,1,4) in(5900) then
             g_ext_swipe_rank := 4;  -- My School card
        end if;
        if substr(g_ext_swipe,1,6) not in(600785,400154,410374,410375,416454,416455,900154,910374,910375)
             and substr(g_ext_swipe,1,4)<>5900 then
             g_ext_swipe_rank := 5;  -- Alien Card
        end if;
        if substr(g_ext_swipe,1,7) in(6007854,6007855) then
             g_ext_swipe_rank := 6;  -- WW Gift card
        end if;
   end if;

   g_tender_no_rank := 9;
   if g_tender_no is not null then

        if substr(g_tender_no,1,8) in(60078501,60078502,60078503,60078504,
             60078506,60078507,60078509,60078521,60078580) then
             g_tender_no_rank := 1;  -- WW store card
        end if;
        if substr(g_tender_no,1,6) in(400154,410374,410375,416454,416455) then
             g_tender_no_rank := 2;  -- WW credit card
        end if;
        if substr(g_tender_no,1,8) in(60078528,60078529,60078530,60078518) then
             g_tender_no_rank := 3;  -- WW wod card
        end if;
        if substr(g_tender_no,1,4) in(5900) then
             g_tender_no_rank := 4;  -- My School card
        end if;
        if substr(g_tender_no,1,6) not in(600785,400154,410374,410375,416454,416455,900154,910374,910375)
             and substr(g_tender_no,1,4)<>5900 then
             g_tender_no_rank := 5;  -- Alien Card
        end if;
        if substr(g_tender_no,1,7) in(6007854,6007855) then
             g_tender_no_rank := 6;  -- WW Gift card
        end if;
        if substr(g_tender_no,1,6) in(100000) then
             g_tender_no_rank := 9;  -- WW voucher THRESVS OR LOYLTV
        end if;
   end if;

   g_pci           := 998;

   if g_tender_no_rank < 9 then
      g_pci := g_tender_no;
   end if;
   if g_ww_swipe_rank  < 9 and
      g_ww_swipe_rank  < g_tender_no_rank then
      g_pci := g_ww_swipe;
   end if;
   if g_ext_swipe_rank  < 9 and
      g_ext_swipe_rank  < g_tender_no_rank and
      g_ext_swipe_rank  < g_ww_swipe_rank then
      g_pci := g_ext_swipe;
   end if;

/*  Tried a case which gave type conflict issues!!
case g_ww_swipe
        when substr(g_ww_swipe,1,8) in(60078501,60078502,60078503,60078504,
             60078506,60078507,60078509,60078521,60078580) then
             g_ww_swipe_rank := 1;  -- WW store card
        when substr(g_ww_swipe,1,6) in(400154,410374,410375,416454,416455) then
             g_ww_swipe_rank := 2;  -- WW credit card
        when substr(g_ww_swipe,1,8) in(60078528,60078529,60078530,60078518) then
             g_ww_swipe_rank := 3;  -- WW wod card
        when substr(g_ww_swipe,1,4) in(5900) then
             g_ww_swipe_rank := 4;  -- My School card
        when substr(g_ww_swipe,1,6) not in(600785,400154,410374,410375,416454,416455,900154,910374,910375)
             and substr(g_ww_swipe,1,4)<>5900 then
             g_ww_swipe_rank := 5;  -- Alien Card
        when substr(g_ww_swipe,1,7) in(6007854,6007855) then
             g_ww_swipe_rank := 6;  -- WW Gift card
        else
             g_ww_swipe_rank := 7;  -- Other ie Voucher or anything else missed in above
       end case;
*/



   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end evaluate_pci_priority;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure write_basket_trans as
begin
       begin
       g_tabl_customer_no := null;
       select  customer_no
       into    g_tabl_customer_no
       from    cust_basket
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date
         and   rownum                        = 1;

       exception
            when no_data_found then
            g_tabl_customer_no := null;
       end;

       if   g_tabl_customer_no is null or g_tabl_customer_no = 0
          then g_tabl_customer_no := g_customer_no;
       end if;

       update  /*+ INDEX (CUST_BASKET PK_F_CUST_BSKT) */ cust_basket
       set     primary_customer_identifier   = g_pci,
               customer_no                   = g_tabl_customer_no
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date;

      g_recs_updated := g_recs_updated  + sql%rowcount;
-----------------------------------------------------------------------
/*       begin
       g_tabl_customer_no := null;
       select  customer_no
       into    g_tabl_customer_no
       from    cust_basket_tender
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date
         and   rownum                        = 1;

       exception
            when no_data_found then
            g_tabl_customer_no := null;
       end;

       if   g_tabl_customer_no is null
          then g_tabl_customer_no := g_customer_no;
       end if;
*/
       update  /*+ INDEX ( CUST_BASKET_TENDER PK_P_CUST_BSKT_TNDR) */ cust_basket_tender
       set     primary_customer_identifier   = g_pci,
               customer_no                   = g_tabl_customer_no
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date;

      g_recs_updated := g_recs_updated  + sql%rowcount;
------------------------------------------------------------------------

/*
       begin
       g_tabl_customer_no := null;
       select  customer_no
       into    g_tabl_customer_no
       from    cust_basket_item
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date
         and   rownum                        = 1;

       exception
            when no_data_found then
            g_tabl_customer_no := null;
       end;

       if   g_tabl_customer_no is null
          then g_tabl_customer_no := g_customer_no;
       end if;
*/
       update  /*+ INDEX ( CUST_BASKET_ITEM PK_P_CUST_BSKT_ITM) */ cust_basket_item
       set     primary_customer_identifier   = g_pci,
               customer_no                   = g_tabl_customer_no
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date;

      g_recs_updated := g_recs_updated  + sql%rowcount;
------------------------------------------------------------------------
/*
       begin
       g_tabl_customer_no := null;
       select  customer_no
       into    g_tabl_customer_no
       from    cust_basket_aux
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date
         and   rownum                        = 1;

       exception
            when no_data_found then
            g_tabl_customer_no := null;
       end;

       if   g_tabl_customer_no is null
          then g_tabl_customer_no := g_customer_no;
       end if;
*/
       update  /*+ INDEX ( CUST_BASKET_AUX PK_P_CUST_BSKT_AUX) */  cust_basket_aux
       set     primary_customer_identifier   = g_pci,
               customer_no                   = g_tabl_customer_no
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date;

      g_recs_updated := g_recs_updated  + sql%rowcount;


   exception
      when others then
       l_message := 'WRITE BASKET TRANS - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end write_basket_trans;

--**************************************************************************************************
-- Update the customer master with date of last transaction
--**************************************************************************************************
procedure write_date_last_transaction as
begin

   if g_customer_no is not null then
       update  dim_customer
       set     last_transaction_date         = g_tran_date
       where   customer_no                   = g_customer_no ;
   end if;

   exception
      when others then
       l_message := 'WRITE LAST TRANSACTION - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end write_date_last_transaction;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin


    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DERIVE PRIMARY CUSTOMER IDENTIFIER STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
for bk_rec in c_prf_cust_basket
    loop

    g_tender_no := null;
    begin
    with bt as
    (select tender_no,
    ( case
        when substr(tender_no,1,8) in(60078501,60078502,60078503,60078504,
            60078506,60078507,60078509,60078521,60078580) then
            1  -- WW store card
        when substr(tender_no,1,6) in(400154,410374,410375,416454,416455) then
            2  -- WW credit card
        when substr(tender_no,1,6) not in(600785,400154,410374,410375,416454,416455,900154,910374,910375)
             and substr(tender_no,1,4)<>5900 then
            5  -- Alien Card
        when substr(tender_no,1,7) in(6007854,6007855) then
            6  -- WW Gift card
        when tender_type_code in('1215','1203','1544','1551') then  --CASH, CHEQUE,VOUCHER,VOUCHER
            9  -- Cash
        else
            7  -- Non cash missed in the above
     end) priority

    from cust_basket_tender
    where location_no     = bk_rec.location_no and
          till_no         = bk_rec.till_no and
          tran_no         = bk_rec.tran_no and
          tran_date       = bk_rec.tran_date
    order by priority)

    select bt.tender_no
    into   g_tender_no
    from  bt
    where rownum = 1;

    exception
            when no_data_found then
            g_tender_no := null;
    end;


    g_ww_swipe         := bk_rec.loyalty_ww_swipe_no;
    g_ext_swipe        := bk_rec.loyalty_ext_swipe_no;
    g_location_no      := bk_rec.location_no;
    g_till_no          := bk_rec.till_no;
    g_tran_no          := bk_rec.tran_no;
    g_tran_date        := bk_rec.tran_date;
    g_online_order_no  := bk_rec.ww_online_order_no;

    g_customer_no := null;
    g_pci         := null;

    if bk_rec.till_no = 999 or g_online_order_no is not null then
       lookup_online_customer_no;
    end if;

    if g_customer_no is null then
       lookup_customer_no;
    end if;

    if g_customer_no is not null then
       g_pci := g_customer_no ;
    else
       evaluate_pci_priority;
    end if;

    write_basket_trans;

--    write_date_last_transaction;

    g_recs_comm := g_recs_comm + 1;
    if g_recs_comm mod 20000 = 0 then
            l_text := 'RECORDS PROCESSED - '||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_comm ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;
    if g_recs_comm mod 100 = 0 then
            commit;
    end if;

    end loop;

    commit;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END "WH_PRF_CUST_108FIX";
