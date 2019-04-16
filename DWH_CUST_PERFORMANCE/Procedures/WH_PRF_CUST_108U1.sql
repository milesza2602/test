--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_108U1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_108U1" (p_forall_limit in integer,p_success out boolean) AS

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
g_recs_other         integer       :=  0;
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


cursor c_prf_cust_basket is
   select /*+ full(cb) parallel (cb,8) */ 
   location_no,till_no,tran_no,tran_date,loyalty_ww_swipe_no,loyalty_ext_swipe_no,ww_online_order_no,customer_no
   from   cust_basket cb
   where  cb.last_updated_date     >  g_date - 2
   and    cb.tran_date             >  g_date - 22
--   and    cb.location_no           between 0 and 437
   ;


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
               rownum = 1 and
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



   exception
      when others then
       l_message := 'LOOKUP OLCUSTOMER NO - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end lookup_online_customer_no;



--**************************************************************************************************
-- Look for a customer number on the master
--**************************************************************************************************
procedure lookup_customer_no as
begin

   g_customer_no := null;

   if g_tender_no is not null and
      (substr(g_tender_no,1,6) in(600785,400154,410374,410375,416454,416455,900154,910374,910375) or
       substr(g_tender_no,1,4) = 5900) then 
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
      g_customer_no is null  and
      (substr(g_ww_swipe,1,6) in(600785,400154,410374,410375,416454,416455,900154,910374,910375) or
       substr(g_ww_swipe,1,4) = 5900) then
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
      g_customer_no is null  and
      (substr(g_ext_swipe,1,6) in(600785,400154,410374,410375,416454,416455,900154,910374,910375) or
       substr(g_ext_swipe,1,4) = 5900)then
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
       l_message := 'LOOKUP NCUSTOMER NO - OTHER ERROR '||sqlcode||' '||sqlerrm;
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

       if   g_tabl_customer_no is null or g_tabl_customer_no = 0
          then g_tabl_customer_no := g_customer_no;
       end if;

       update  /*+ INDEX (CUST_BASKET PK_F_CUST_BSKT) */ cust_basket
       set     primary_customer_identifier   = g_pci,
               customer_no                   = g_tabl_customer_no 
       where   location_no                   = g_location_no
         and   till_no                       = g_till_no
         and   tran_no                       = g_tran_no
         and   tran_date                     = g_tran_date
         and  (primary_customer_identifier <> g_pci or nvl(customer_no,0) <> nvl(g_tabl_customer_no,0)) 
--         and   primary_customer_identifier   = 0
         ;

      g_recs_updated := g_recs_updated  + sql%rowcount;


   exception
      when others then
       l_message := 'WRITE BASKET TRANS - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end write_basket_trans;



--**************************************************************************************************
-- Update the customer master with date of last transaction, online transaction & vitality purchase
--**************************************************************************************************
procedure write_last_xxxx_date  as
begin

    l_text := 'TAKE ON LTD '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


MERGE  INTO dim_customer cust
   USING (
   select /*+ full(cb) parallel (cb,12) */ 
   cb.customer_no,
   max(cb.tran_date) ltd
   from   cust_basket cb 
   where  cb.tran_date    BETWEEN g_date-31 and g_date 
   and    cb.last_updated_date > g_date-5
   and    cb.customer_no  is not null
   group by cb.customer_no
         ) mer_rec
   ON    (  cust.	customer_no	     =	mer_rec.	customer_no )
   WHEN MATCHED THEN 
   UPDATE SET cust.	last_transaction_date =	mer_rec.	ltd	
   WHERE      mer_rec.ltd  > nvl(cust.last_transaction_date,'1 Jan 2000')  ;   

   g_recs_inserted := g_recs_inserted  + sql%rowcount; 

   l_text := 'TAKE ON LOD '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

COMMIT;

MERGE  INTO dim_customer cust
   USING (
   select /*+ full(cb) parallel (cb,12) */ 
   cb.customer_no,
   max(cb.tran_date) lod
   from   cust_basket cb 
   where  (cb.till_no = 999 or cb.till_no = 997 or cb.ww_online_order_no is not null)
   and    cb.tran_date    BETWEEN g_date-31 and g_date            
   and    cb.customer_no  is not null
   group by cb.customer_no
         ) mer_rec
   ON    (  cust.	customer_no	     =	mer_rec.	customer_no )
   WHEN MATCHED THEN 
   UPDATE SET cust.	last_online_date =	mer_rec.	lod	
   WHERE      mer_rec.lod  > nvl(cust.last_online_date,'1 Jan 2000')  ;   
 
   g_recs_inserted := g_recs_inserted  + sql%rowcount;   

   l_text := 'TAKE ON LVD '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

COMMIT;

MERGE  INTO dim_customer cust
   USING (
   select /*+ full(cb) parallel (cb,12)  */ 
   cb.customer_no,
   max(cb.tran_date) lvd
   from   cust_basket_item cb
   where  vitality_cust_ind = 1 and vitality_uda_value = 1
   and    cb.tran_date    BETWEEN  g_date-31 and g_date 
   and    cb.customer_no  is not null
   group by cb.customer_no 
         ) mer_rec
   ON    (  cust.	customer_no	     =	mer_rec.	customer_no )
   WHEN MATCHED THEN 
   UPDATE SET cust. last_vitality_date =	mer_rec.	lvd	
   WHERE      mer_rec.lvd  > nvl(cust.last_vitality_date,'1 Jan 2000') ;    


   g_recs_inserted := g_recs_inserted  + sql%rowcount;

COMMIT;
   
   exception
      when others then
       l_message := 'WRITE LAST TRANSACTION - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end write_last_xxxx_date;

--**************************************************************************************************
-- Update the customer master with date of last transaction, online transaction & vitality purchase
-- ALTERNATIVE SOLUTION TO ABOVE DONE IN ONE PASS IF ABOVE NOT PERFORMING
--**************************************************************************************************
procedure write_date_last_trans  as
begin

MERGE  INTO dim_customer cust
   USING (

WITH BSKT AS
   (
   select /*+ full(cb) parallel (cb,8) PARALLEL(DC,8) */ 
   cb.customer_no,
   max(
   case 
   when cb.tran_date > nvl(last_transaction_date,'1 Jan 2000') 
   then cb.tran_date 
   else last_transaction_date
   end ) ltd ,
   max(
   case 
   when (cb.till_no = 999 or cb.till_no = 997 or cb.ww_online_order_no is not null) 
   and  cb.tran_date > nvl(last_online_date,'1 Jan 2000')   
   then cb.tran_date
   else last_online_date   
   end ) lod
   from   cust_basket cb, dim_customer dc
   where  cb.last_updated_date     >  g_date - 2
   and    cb.tran_date             >  g_date - 22
   and    cb.customer_no  is not null
   and    cb.customer_no           = dc.customer_no 
   group by cb.customer_no
   ),
     BSKTI AS
   (  
   select /*+ full(cb) parallel (cb,8) PARALLEL(DC,8) */ 
   cb.customer_no,
   max(
   case 
   when vitality_cust_ind = 1 and vitality_uda_value = 1
   and  cb.tran_date > nvl(last_vitality_date,'1 Jan 2000')    
   then cb.tran_date
   else last_vitality_date     
   end) lvd
   from   cust_basket_item cb, dim_customer dc
   where  cb.last_updated_date     >  g_date - 2
   and    cb.tran_date             >  g_date - 22
   and    cb.customer_no  is not null
   and    cb.customer_no           = dc.customer_no 
   group by cb.customer_no )
  
   select b.* ,bi.lvd
   from bskt b,
        bskti bi
   where  b.customer_no = bi.customer_no ) mer_rec
   
   ON    (  cust.	customer_no	          =	mer_rec.	customer_no )
   WHEN MATCHED THEN 
   UPDATE SET
            cust.	last_transaction_date   =	mer_rec.	ltd	,
            cust.	last_online_date	      =	mer_rec.	lod	,
            cust. last_vitality_date      = mer_rec.	lvd
   ;
   
   g_recs_inserted := g_recs_inserted  + sql%rowcount;
   
   exception
      when others then
       l_message := 'WRITE LAST TRANSACTION - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end write_date_last_trans;

--**************************************************************************************************
-- Update the customer master with date of last transaction, online transaction & vitality purchase
-- ALTERNATIVE SOLUTION TO ABOVE DONE IN ONE PASS IF ABOVE NOT PERFORMING
--**************************************************************************************************
procedure write_other_baskets  as
begin

MERGE  INTO /*+ parallel (cbt,8) */ cust_basket_tender cbt
   USING (
         select /*+ full(cb) parallel (cb,8) */
                location_no,till_no,tran_no,tran_date,customer_no,primary_customer_identifier
         from   cust_basket cb
         where  cb.last_updated_date     >  g_date - 2
         and    cb.tran_date             >  g_date - 22) mer_rec
   ON    (      cbt.location_no             = mer_rec.location_no
         and    cbt.till_no                 = mer_rec.till_no
         and    cbt.tran_no                 = mer_rec.tran_no
         and    cbt.tran_date               = mer_rec.tran_date)
   WHEN MATCHED THEN 
   UPDATE SET
                cbt.primary_customer_identifier  =	mer_rec.primary_customer_identifier,
                cbt.customer_no	                 =	mer_rec.customer_no	 
   WHERE        nvl(cbt.primary_customer_identifier,0) <> mer_rec.primary_customer_identifier       
   ;
   
   g_recs_other := g_recs_other  + sql%rowcount;
   COMMIT;
   
   l_text := 'WRITE OTHER BASKETS T - '||G_RECS_OTHER||' '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
------------------------------------------------------------------------

   MERGE  INTO /*+ parallel (cbi,8) */ cust_basket_item cbi
   USING (
         select /*+ full(cb) parallel (cb,8) */
                location_no,till_no,tran_no,tran_date,customer_no,primary_customer_identifier
         from   cust_basket cb
         where  cb.last_updated_date     >  g_date - 2
         and    cb.tran_date             >  g_date - 22) mer_rec
   ON    (      cbi.location_no             = mer_rec.location_no
         and    cbi.till_no                 = mer_rec.till_no
         and    cbi.tran_no                 = mer_rec.tran_no
         and    cbi.tran_date               = mer_rec.tran_date)
   WHEN MATCHED THEN 
   UPDATE SET
                cbi.primary_customer_identifier  =	mer_rec.primary_customer_identifier,
                cbi.customer_no	                 =	mer_rec.customer_no	 
   WHERE        nvl(cbi.primary_customer_identifier,0) <> mer_rec.primary_customer_identifier       
   ;

   g_recs_other :=  g_recs_other  + sql%rowcount;
   COMMIT;
   
   l_text := 'WRITE OTHER BASKETS I - '||G_RECS_OTHER||' '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
------------------------------------------------------------------------

   MERGE  INTO /*+ parallel (cbi,8) */ cust_basket_aux cba
   USING (
         select /*+ full(cb) parallel (cb,8) */
                location_no,till_no,tran_no,tran_date,customer_no,primary_customer_identifier
         from   cust_basket cb
         where  cb.last_updated_date     >  g_date - 2
         and    cb.tran_date             >  g_date - 22) mer_rec
   ON    (      cba.location_no             = mer_rec.location_no
         and    cba.till_no                 = mer_rec.till_no
         and    cba.tran_no                 = mer_rec.tran_no
         and    cba.tran_date               = mer_rec.tran_date)
   WHEN MATCHED THEN 
   UPDATE SET
                cba.primary_customer_identifier  =	mer_rec.primary_customer_identifier,
                cba.customer_no	                 =	mer_rec.customer_no	 
   WHERE        nvl(cba.primary_customer_identifier,0) <> mer_rec.primary_customer_identifier       
   ;

   
   g_recs_other := g_recs_other  + sql%rowcount;
   COMMIT;
   
   l_text := 'WRITE OTHER BASKETS A - '||G_RECS_OTHER||' '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   

   exception
      when others then
       l_message := 'WRITE OTHER BASKETS - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end write_other_baskets;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';

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
    
execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = false';

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
    g_tabl_customer_no := bk_rec.customer_no;
    g_customer_no := null;
    g_pci         := null;

    if bk_rec.till_no = 999 or bk_rec.till_no = 997 or g_online_order_no is not null then
       lookup_online_customer_no;
    end if;

    if g_customer_no is null and 
       g_tabl_customer_no is not null and g_tabl_customer_no <>  0 then
       g_customer_no := g_tabl_customer_no;
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
    
--    write_date_last_transaction;      **** old code inline for last transaction date - changed to multiple dates in a BULK merge at end ***

    g_recs_comm := g_recs_comm + 1;
    if g_recs_comm mod 20000 = 0 then
            l_text := 'RECORDS PROCESSED NEW- '||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_comm ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;
    if g_recs_comm mod 1000 = 0 then
            commit;
    end if;
    
    end loop;

    commit;
    
--**************************************************************************************************
-- At end write other baskets in a bulk merge process
--**************************************************************************************************   

    l_text := 'START PROCESS TO WRITE OTHER BASKETS - '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   

    write_other_baskets; 
    
    commit;
    
--**************************************************************************************************
-- At end write various last transaction dates to customer master
--**************************************************************************************************   

    l_text := 'START PROCESS TO WRITE LAST TRANSACTION DATES - '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   

--    write_date_last_trans;
    
    write_last_xxxx_date;
    
    commit;
    
--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS UPDATED TO OTHER BASKETS '||g_recs_other;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||'108U1'||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
        execute immediate 'alter session set "_optimizer_star_tran_in_with_clause" = true';
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

END WH_PRF_CUST_108U1;
