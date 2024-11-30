from pyspark.sql import SparkSession, Row
import argparse
from pyspark.sql.window import Window
from pyspark.sql.functions import col, substring, lit, expr, regexp_replace, when, coalesce, row_number, desc, round, \
    to_date, date_format, current_timestamp, current_date
from datetime import timedelta, datetime
from pyspark.sql.types import StructType, StructField, StringType
from persiantools.jdatetime import JalaliDate


class RemainedDebt:
    def __init__(self, param_data_day_c, param_master_c, param_hdfs_addr_c):
        self.param_data_day_c = param_data_day_c
        self.param_master_c = param_master_c
        self.param_hdfs_addr_c = param_hdfs_addr_c

    def run_spark(self):
        # sys_arg = ['20211008', 'local[*]', '172.18.60.9:8020']
        param_data_day = self.param_data_day_c
        param_master = self.param_master_c
        param_hdfs_addr = self.param_hdfs_addr_c
        date_object = datetime.strptime(param_data_day, "%Y%m%d")
        data_day = str(date_object).split()[0]
        spark = SparkSession.builder.appName(
            'Remained_Debt').master(param_master).getOrCreate()
        spark.udf.registerJavaFunction(
            "persian_date", "Persian_Date", StringType())
        inf_account = spark.read \
            .option("header", True) \
            .parquet("hdfs://" + param_hdfs_addr + "/user/bdi/CRM/INPUT_DATA/INF_ACCOUNT/" + param_data_day + "/**/**") \
            # .where(col('C_EX_FIELD4').isNotNull())

        bc_acct = spark.read \
            .option("header", True) \
            .parquet("hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/BC_ACCT/" + param_data_day + "/**/**") \
            .select('TP_ACCT_KEY', 'ACCT_ID', 'PAYMENT_TYPE')

        temp = bc_acct.alias('a') \
            .join(inf_account.alias('b'),
                  col('a.TP_ACCT_KEY').cast('string') == col('b.acct_id').cast('string'),
                  'left_outer') \
            .where(col('a.PAYMENT_TYPE') == 1)

        t_m_rpt_charity_sum_1 = temp.withColumn('province_id', when((col("C_EX_FIELD4") == lit("0")) |
                                                                    (col("C_EX_FIELD4").isNull()),
                                                                    lit("4380")).otherwise(col("C_EX_FIELD4"))) \
            .select(col('a.acct_id'), 'province_id')

        ar_invoice = spark.read \
            .option("header", True) \
            .parquet("hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE/" + param_data_day + "/**/**") \
            .select(col("ACCT_ID"),
                    col("INVOICE_STATUS"),
                    col('BILL_CYCLE_ID'),
                    col('INVOICE_TYPE'),
                    col('trans_type')) \
            .where(col('trans_type').isin(['BLL'])) \
            .where(col('invoice_type').isin(['5'])) \
            .where(col('bill_cycle_id') > 0)

        param_data_dat_date_obj = datetime.strptime(param_data_day, '%Y%m%d')
        cycle = (param_data_dat_date_obj.replace(day=1) - timedelta(days=1)).replace(day=1)
        a = cycle.strftime('%Y%m%d')
        b = a[:6]
        param_data_day_date = JalaliDate(datetime.strptime(param_data_day, '%Y%m%d'))
        last_cycle_first_day_jalali = (param_data_day_date.replace(day=1) - timedelta(days=1)).replace(day=1)
        last_cycle_month_formatted_shamsi = last_cycle_first_day_jalali.strftime('%Y%m')

        t_d_dim_dic_bill_cycle_raw = spark.read \
            .option("header", True) \
            .csv("hdfs://" + param_hdfs_addr + "/user/bdi/dimensions/T_D_DIM_DIC_BILL_CYCLE.csv")

        t_d_dim_dic_bill_cycle = t_d_dim_dic_bill_cycle_raw \
            .where(to_date(col('next_bill_cycle_id'), 'yyyyMMdd') == to_date(lit(a), 'yyyyMMdd'))

        temp = ar_invoice.alias('a').join(t_d_dim_dic_bill_cycle.alias('b'),
                                          col('a.BILL_CYCLE_ID') <= col('b.next_bill_cycle_id'))
        t_m_rpt_charity_sum_2 = temp.select(
            col('acct_id'),
            col('invoice_status'),
            col('a.bill_cycle_id'),
            col('b.bill_cycle_id').alias('last_bill_cycle'),
            col('next_bill_cycle_id').alias('bill_cycle_id_1')
        )

        t_m_rpt_charity_sum_3 = t_m_rpt_charity_sum_2.alias('a') \
            .join(t_m_rpt_charity_sum_1.alias('b'), col('a.acct_id') == col('b.acct_id'), 'left_outer') \
            .select(
            col('bill_cycle_id_1'),
            col('last_bill_cycle'),
            col('a.acct_id'),
            col('invoice_status'),
            col('bill_cycle_id'),
            col('province_id')
        )

        dim_charity = spark.read \
            .option("header", True) \
            .csv("hdfs://" + param_hdfs_addr + "/user/bdi/dimensions/t_d_dim_charity_service_org.csv")

        dim_charity_1 = dim_charity.where(
            col('data_month') == b
        )

        cdr_com = spark.read \
            .option('header', True) \
            .parquet(
            "hdfs://" + param_hdfs_addr + '/user/bdi/CDR/CLEAN/COM/*/' + '*' + last_cycle_month_formatted_shamsi)

        mgr_cdr = spark.read \
            .option('header', True) \
            .parquet(
            "hdfs://" + param_hdfs_addr + '/user/bdi/CDR/CLEAN/MGR/*/' + '*' + last_cycle_month_formatted_shamsi)

        mgr_cdr_1 = mgr_cdr.where(
            col('paytype') == '1'
        )

        union1 = mgr_cdr_1.alias('a') \
            .join(dim_charity_1.alias('b'), col('CHARITY_ORGANIZATION_CODE') == col('service_id')) \
            .select(
            col('BILL_CYCLE_ID'),
            col('DEFAULT_ACCT_ID').alias('ACCT_ID'),
            col('DEBIT_AMOUNT').alias('DEBIT_FEE'),
            col('service_id').alias('ORG_CODE'),
            col('organization_name').alias('ORG_NAME')
        )

        union2 = cdr_com.alias('a') \
            .join(dim_charity_1.alias('b'), col('SERVICEID') == col('service_id'), 'left_outer') \
            .select(
            col('BILL_CYCLE_ID'),
            col('DEFAULT_ACCT_ID').alias('ACCT_ID'),
            coalesce(col('DEBIT_AMOUNT') - col('VAT_TAX') - col('DUTY_TAX'), lit(0)).alias('DEBIT_FEE'),
            col('SERVICEID').alias('ORG_CODE'),
            col('organization_name').alias('ORG_NAME')
        )

        t_m_rpt_charity_sum_4 = union1.union(union2)

        t_m_rpt_charity_sum_5 = t_m_rpt_charity_sum_3 \
            .alias('a') \
            .join(t_m_rpt_charity_sum_4.alias('b'),
                  (
                          (col('a.bill_cycle_id_1') == col('a.bill_cycle_id'))
                          & (col('a.bill_cycle_id') == col('b.bill_cycle_id'))
                          & (col('a.acct_id') == col('b.acct_id'))
                  )
                  ).select(
            col('a.bill_cycle_id_1'),
            col('a.acct_id'),
            col('a.invoice_status'),
            col('a.bill_cycle_id'),
            col('a.province_id'),
            coalesce(col('b.DEBIT_FEE'), lit(0)).alias('DEBIT_FEE'),
            col('b.ORG_CODE'),
            col('b.ORG_NAME'),
            lit(0).alias('PRE_PAID_FEE1'),
            lit(0).alias('PRE_UNPAID_FEE1')
        ) \
            .withColumn('CUR_PAID_FEE1',
                        when(col('invoice_status') == lit('C'), coalesce(col('DEBIT_FEE'), lit(0)))) \
            .withColumn('CUR_UNPAID_FEE1',
                        when(col('invoice_status') == lit('O'), coalesce(col('DEBIT_FEE'), lit(0))))
        t_m_rpt_charity_sum_5.createOrReplaceTempView("t_m_rpt_charity_sum_5")

        t_m_rpt_charity_sum_6 = t_m_rpt_charity_sum_5 \
            .groupBy(
            col('bill_cycle_id_1'),
            col('bill_cycle_id'),
            col('province_id'),
            col('ORG_CODE'),
            col('ORG_NAME')
        ).agg(
            sum(coalesce(col('PRE_PAID_FEE1'), lit(0))).alias('PRE_PAID_FEE'),
            sum(coalesce(col('PRE_UNPAID_FEE1'), lit(0))).alias('PRE_UNPAID_FEE'),
            sum(coalesce(col('CUR_PAID_FEE1'), lit(0))).alias('CUR_PAID_FEE'),
            sum(coalesce(col('CUR_UNPAID_FEE1'), lit(0))).alias('CUR_UNPAID_FEE'),
        )

        t_l_chr_m = spark.read.option('header', True).csv('/user/bdi/CDR/INTERMEDIATE/T_L_CHARITY_M/202301/*') \
            .where(col('bill_cycle_id') != lit('20230101'))
        t_l_chr_m.createOrReplaceTempView("t_l_charity_m")

        ###-------------------------###

        t_m_rpt_charity_sum_7 = t_m_rpt_charity_sum_3.alias('a') \
            .join(t_l_chr_m.alias('b'),
                  (
                          (col('a.bill_cycle_id') < col('a.bill_cycle_id_1'))
                          & (col('a.bill_cycle_id') == col('b.bill_cycle_id'))
                          & (col('a.acct_id') == col('b.acct_id'))
                      # & (col('a.province_id') == col('b.province_id'))
                  )
                  ) \
            .where(col('a.invoice_status') == lit('C')) \
            .where(col('a.bill_cycle_id') < col('a.bill_cycle_id_1'))\
            .select(
            col('a.bill_cycle_id_1').alias('CURR_bill_cycle_id'),
            col('a.last_bill_cycle'),
            col('a.acct_id'),
            col('a.invoice_status'),
            col('a.bill_cycle_id'),
            col('a.province_id'),
            col('b.debit_fee'),
            col('b.org_code'),
            col('b.org_name')
        )
        t_l_charity_m_1 = t_m_rpt_charity_sum_5.where(col('invoice_status') == lit('C'))\
            .select('bill_cycle_id', 'acct_id', 'org_name', 'org_code', 'province_id', 'debit_fee', 'invoice_status')

        t_l_charity_m_2 = t_l_chr_m.alias('a').join(
            t_l_charity_m_1.alias('b'), (
                          (col('a.bill_cycle_id') == col('b.bill_cycle_id'))
                          & (col('a.org_name') == col('b.org_name'))
                          & (col('a.acct_id') == col('b.acct_id'))
                          & (col('a.org_code') == col('b.org_code'))
                  ), 'left_outer'
        ).select(
            col('data_month'),
            col('bill_cycle_id'),
            col('acct_id'),
            col('province_id'),
            col('debit_fee'),
            col('org_code'),
            col('org_name'),
            coalesce(col('b.invoice_status'), col('a.invoice_status')).alias('invoice_status'),
            col('in_month'),
            col('charge_fee'),
            col('procs_time')
        )

        t_m_rpt_charity_sum_8_1 = t_l_charity_m_2.alias('a') \
            .join(t_m_rpt_charity_sum_7.alias('b'),
                  (
                          (col('a.bill_cycle_id') == col('b.bill_cycle_id'))
                          & (col('a.acct_id') == col('b.acct_id'))
                  ), 'left_outer'
                  ) \
            .select(
            col('a.bill_cycle_id'), col('a.acct_id'),
            col('a.province_id'), col('a.debit_fee'),
            col('a.org_code'), col('a.org_name'),
            col('a.invoice_status'), col('b.acct_id').alias('acct_id1')) \
            .where(col('acct_id1').isNull())

        t_m_rpt_charity_sum_8 = t_m_rpt_charity_sum_8_1.select(
            col('bill_cycle_id'), col('acct_id'), col('a.province_id'), col('a.debit_fee'),
            col('a.org_code'), col('a.org_name'), col('a.invoice_status')
        )

        inter_output = t_m_rpt_charity_sum_8.select(
            lit(b).alias('DATA_MONTH'),
            col('bill_cycle_id').alias('BILL_CYCLE_ID'),
            col('acct_id').alias('ACCT_ID'),
            col('province_id').alias('PROVINCE_ID'),
            col('debit_fee').alias('DEBIT_FEE'),
            col('org_code').alias('ORG_CODE'),
            col('org_name').alias('ORG_NAME'),
            col('invoice_status').alias('INVOICE_STATUS'),
            lit('').alias('IN_MONTH'),
            lit('0').alias('CHARGE_FEE'),
            date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss').alias('PROCS_TIME')
        )

        last_run = spark.read.parquet("hdfs://" + param_hdfs_addr + f'/user/bdi/REPORT_RESULT/CHARITY_SUM/{b}/*')

        step_1_9 = last_run.alias('a') \
            .join(t_d_dim_dic_bill_cycle_raw.alias('b'),
                  col('a.bill_cycle_id') == col('b.bill_cycle_id')) \
            .join(t_d_dim_dic_bill_cycle_raw.alias('c'),
                  col('b.next_bill_cycle_id') == col('c.bill_cycle_id')) \
            .where(col('c.in_month') == lit(b))

        t_m_rpt_charity_sum_9 = step_1_9.select(
            col('a.bill_cycle_id'),
            col('a.province_id'),
            col('a.org_code'),
            col('a.org_name'),
            col('a.cur_paid_fee'),
            col('a.cur_unpaid_fee'),
            col('a.pre_paid_fee'),
            col('a.pre_unpaid_fee'),
            col('a.bill_cycle_id_1'),
            col('c.bill_cycle_id').alias('cur_bill_cycle_id'),
        )

        t = t_m_rpt_charity_sum_9

        tt = t_m_rpt_charity_sum_7 \
            .groupBy("last_bill_cycle", "bill_cycle_id", "province_id", "org_code", "org_name") \
            .agg(sum("debit_fee").alias("debit_fee_sum"))

        t_m_rpt_charity_sum_10 = t.join(tt, (
                (t.bill_cycle_id == tt.last_bill_cycle) &
                (t.province_id == tt.province_id) &
                (t.org_code == tt.org_code) &
                (t.org_name == tt.org_name) &
                (t.bill_cycle_id_1 == tt.bill_cycle_id)
        ),
                                        "left_outer") \
            .select(
            t.province_id,
            t.org_code,
            t.org_name,
            (t.cur_paid_fee + coalesce(tt.debit_fee_sum, lit(0))).alias("cur_paid_fee"),
            (t.cur_unpaid_fee - coalesce(tt.debit_fee_sum, lit(0))).alias("cur_unpaid_fee"),
            t.pre_paid_fee,
            t.pre_unpaid_fee,
            t.bill_cycle_id_1,
            t.cur_bill_cycle_id.alias("BILL_CYCLE_ID")
        )

        t_m_rpt_charity_sum_11 = t_m_rpt_charity_sum_9.alias('a') \
            .join(tt.alias('b'),
                  (col("a.bill_cycle_id") == col("last_bill_cycle")) &
                  (col("a.province_id") == col("b.province_id")) &
                  (col("a.org_code") == col("b.org_code")) &
                  (col("a.org_name") == col("b.org_name")), "left_outer") \
            .where(col("a.bill_cycle_id_1") == col("a.bill_cycle_id")) \
            .select(
            col("a.province_id"),
            col("a.org_code"),
            col("a.org_name"),
            col("CUR_BILL_CYCLE_ID").alias("BILL_CYCLE_ID"),
            coalesce(col("debit_fee_sum"), lit(0)) + coalesce(col("PRE_PAID_FEE"), lit(0)) + coalesce(
                col("cur_paid_fee"), lit(0)).alias("PRE_PAID_FEE"),
            coalesce(col("PRE_UNPAID_FEE"), lit(0)) + coalesce(col("cur_unpaid_fee"), lit(0)) - coalesce(
                col("debit_fee_sum"), lit(0)).alias("PRE_UNPAID_FEE"),
            lit(0).alias("CUR_PAID_FEE"),
            lit(0).alias("CUR_UNPAID_FEE"),
            col("CUR_BILL_CYCLE_ID").alias("BILL_CYCLE_ID_1")
        )

        res_union_1 = t_m_rpt_charity_sum_11.select(
            'province_id',
            'org_code',
            'org_name',
            'BILL_CYCLE_ID',
            'PRE_PAID_FEE',
            'PRE_UNPAID_FEE',
            'CUR_PAID_FEE',
            'CUR_UNPAID_FEE',
            'BILL_CYCLE_ID_1'
        )
        res_union_2 = t_m_rpt_charity_sum_10.select(
            'province_id',
            'org_code',
            'org_name',
            'BILL_CYCLE_ID',
            'PRE_PAID_FEE',
            'PRE_UNPAID_FEE',
            'CUR_PAID_FEE',
            'CUR_UNPAID_FEE',
            'BILL_CYCLE_ID_1'
        )
        res_union_3 = t_m_rpt_charity_sum_6.select(
            'province_id',
            'org_code',
            'org_name',
            'BILL_CYCLE_ID',
            'PRE_PAID_FEE',
            'PRE_UNPAID_FEE',
            'CUR_PAID_FEE',
            'CUR_UNPAID_FEE',
            'BILL_CYCLE_ID_1'
        )
        union_result = (res_union_1.union(res_union_2)).union(res_union_3)
        t_m_rpt_charity_sum_m = union_result \
            .withColumn("BILL_CYCLE_ID_1", date_format(current_date(), "yyyyMMdd")) \
            .groupBy(
            'BILL_CYCLE_ID',
            'province_id',
            'org_code',
            'org_name',
            'BILL_CYCLE_ID_1'
        ).agg(
            sum(coalesce(col('CUR_PAID_FEE'), lit(0))),
            sum(coalesce(col('CUR_UNPAID_FEE'), lit(0))),
            sum(coalesce(col('PRE_PAID_FEE'), lit(0))),
            sum(coalesce(col('PRE_UNPAID_FEE'), lit(0))),
        ) \
            .withColumn("DATA_MONTH", lit(b))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('arguments', nargs='+',
                        help='an integer for the accumulator')
    args = parser.parse_args()
    sys_arg = vars(args).get('arguments')
    param_data_day_int = sys_arg[0]
    param_master_int = sys_arg[1]
    param_hdfs_addr_int = sys_arg[2]

    instance = RemainedDebt(param_data_day_int, param_master_int, param_hdfs_addr_int)
    instance.run_spark()
