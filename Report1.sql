create table t_m_rpt_part_payment_01 as
select invoice_id, ACCT_ID, PRI_IDENTITY, INVOICE_AMT/1000 INVOICE_FEE, open_amt/1000 OPEN_FEE, BILL_CYCLE_ID, (INVOICE_AMT-open_amt)/1000 TRANS_AMT
	from ar_invoice
	where INVOICE_TYPE = '5' 
	and (AOD_STATUS != 'Y' or AOD_STATUS is null);
	
create table t_m_rpt_part_payment_02 AS
select tt.ACCT_ID CBS_ACCT_ID, t.payment_type payment_mode, decode(nvl(t.c_ex_field4,'0'),'0','4380',c_ex_field4) PROVINCE_ID
       from inf_account t left outer join bc_acct tt on to_char(t.acct_id) = tt.TP_ACCT_KEY;

create table t_m_rpt_part_payment_03 as
select t.invoice_id, t.INVOICE_FEE, t.OPEN_FEE, t.BILL_CYCLE_ID, t.TRANS_AMT, tt.payment_mode, tt.PROVINCE_ID
	from t_m_rpt_part_payment_01 t left outer join t_m_rpt_part_payment_02 tt
	on t.acct_id = tt.cbs_acct_id
	where PAYMENT_MODE = '1' 
	and INVOICE_FEE != OPEN_FEE 
	and OPEN_FEE >= 1000;

create table t_m_rpt_part_payment_04 as
select invoice_id
	from ar_invoice -- previous_cycle
	where INVOICE_AMT = open_amt 
	and INVOICE_TYPE=='5' 
	and (AOD_STATUS != 'Y' or AOD_STATUS is null) 
	and BILL_CYCLE_ID < to_char(add_month(sysdate,-1),'yyyymm')||'01';

create table t_m_rpt_part_payment_05_1 as
select BILL_CYCLE_ID, PROVINCE_ID, count(INVOICE_ID) PART_CNT, sum(TRANS_AMT) PART_PAY_FEE, sum(INVOICE_FEE) PART_INV_FEE
	from t_m_rpt_part_payment_03
		group by BILL_CYCLE_ID, PROVINCE_ID;

create table t_m_rpt_part_payment_05_2 as
select t.BILL_CYCLE_ID BILL_CYCLE_ID1, t.PROVINCE_ID PROVINCE_ID1, t.PART_CNT PART_CNT1, t.PART_PAY_FEE PART_PAY_FEE1, t.PART_INV_FEE PART_INV_FEE1, tt.BILL_CYCLE_ID, tt.PROVINCE_ID, tt.PART_CNT, tt.PART_PAY_FEE, tt.PART_INV_FEE, tt.CHANGE_CNT, tt.CHANGE_PAY_FEE, tt.CHANGE_INV_FEE, tt.PAID_FLAG
	from t_m_rpt_part_payment_05_1 t full outer join t_m_rpt_part_payment_d tt on t.BILL_CYCLE_ID = tt.BILL_CYCLE_ID and t.PROVINCE_ID = tt.PROVINCE_ID; -- previous cycle

create table t_m_rpt_part_payment_05 as
select nvl(BILL_CYCLE_ID,BILL_CYCLE_ID1) BILL_CYCLE_ID,	nvl(PROVINCE_ID,PROVINCE_ID1) PROVINCE_ID, NVL(PART_CNT1,0) PART_CNT, NVL(PART_PAY_FEE1,0) PART_PAY_FEE, NVL(PART_INV_FEE1,0) PART_INV_FEE, 0 CHANGE_CNT, 0 CHANGE_PAY_FEE, 0 CHANGE_INV_FEE, '1' PAID_FLAG from t_m_rpt_part_payment_05_2;

create table t_m_rpt_part_payment_06_1 as
select t.BILL_CYCLE_ID, t.PROVINCE_ID, t.INVOICE_ID, t.TRANS_AMT, t.INVOICE_FEE
	from t_m_rpt_part_payment_03 t inner join t_m_rpt_part_payment_04 tt
	on t.invoice_id = tt.invoice_id;
	
create table t_m_rpt_part_payment_06 as
select 	BILL_CYCLE_ID, PROVINCE_ID, count(INVOICE_ID) CHANGE_CNT, sum(TRANS_AMT) CHANGE_PAY_FEE, sum(INVOICE_FEE) CHANGE_INV_FEE, 0 PART_CNT, 0 PART_PAY_FEE, 0 PART_INV_FEE, '1' PAID_FLAG
	from t_m_rpt_part_payment_06_1
	group by BILL_CYCLE_ID, PROVINCE_ID;

create table t_m_rpt_part_payment_07 as
select BILL_CYCLE_ID, PROVINCE_ID, PART_CNT, PART_PAY_FEE, PART_INV_FEE, CHANGE_CNT, CHANGE_PAY_FEE, CHANGE_INV_FEE, PAID_FLAG from t_m_rpt_part_payment_05
UNION ALL
select BILL_CYCLE_ID, PROVINCE_ID, PART_CNT, PART_PAY_FEE, PART_INV_FEE, CHANGE_CNT, CHANGE_PAY_FEE, CHANGE_INV_FEE, PAID_FLAG from t_m_rpt_part_payment_06;

create table t_m_rpt_part_payment_d as
select BILL_CYCLE_ID, PROVINCE_ID, PAID_FLAG, sum(PART_CNT) PART_CNT, sum(PART_PAY_FEE) PART_PAY_FEE, sum(PART_INV_FEE) PART_INV_FEE, sum(CHANGE_CNT) CHANGE_CNT, sum(CHANGE_PAY_FEE) CHANGE_PAY_FEE, sum(CHANGE_INV_FEE) CHANGE_INV_FEE, to_char(sysdate,'yyyymmdd') DATA_DAY, sysdate procs_time
from t_m_rpt_part_payment_07 group by BILL_CYCLE_ID, PROVINCE_ID;

