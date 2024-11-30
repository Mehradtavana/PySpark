from pyspark.sql import SparkSession, Row
import argparse
from pyspark.sql.window import Window
from pyspark.sql.functions import col, substring, lit, expr, regexp_replace, when, coalesce, row_number, desc, round, \
    to_date, date_format, current_timestamp, current_date, sum, get_json_object, abs
from datetime import timedelta, datetime
from pyspark.sql.types import StructType, StructField, StringType
from persiantools.jdatetime import JalaliDate


class Debt:
    def __init__(self, param_data_day_c, param_master_c, param_hdfs_addr_c, param_oracle_addr_c, param_oracle_user_c, param_oracle_pass_c):
        self.param_data_day_c = param_data_day_c
        self.param_master_c = param_master_c
        self.param_hdfs_addr_c = param_hdfs_addr_c
        self.param_oracle_addr_c = param_oracle_addr_c
        self.param_oracle_user_c = param_oracle_user_c
        self.param_oracle_pass_c = param_oracle_pass_c

    def run_spark(self):
        # sys_arg = ['20211008', 'local[*]', '172.18.60.9:8020']
        param_data_day = self.param_data_day_c
        param_master = self.param_master_c
        param_hdfs_addr = self.param_hdfs_addr_c
        param_oracle_addr = self.param_oracle_addr_c
        param_oracle_user = self.param_oracle_user_c
        param_oracle_pass = self.param_oracle_pass_c
        
        date_object = datetime.strptime(param_data_day, "%Y%m%d")
        data_day = str(date_object).split()[0]
        spark = SparkSession.builder.appName(
            'debt_condition').master(param_master).getOrCreate()
        spark.udf.registerJavaFunction(
            "persian_date", "Persian_Date", StringType())

        str_bill_cycle = (date_object.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y%m%d')
        param_data_month = (date_object.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y%m')
        
        
        T_D_DIM_DIC_BILL_CYCLE = spark.read \
            .option("header", True) \
            .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/dimensions/T_D_DIM_DIC_BILL_CYCLE.csv")

        ThisMonth = T_D_DIM_DIC_BILL_CYCLE.where(
            to_date(lit(PARAM_DATA_DAY), "yyyyMMdd") < to_date(col("END_DATE"), "yyyyMMddHHmmss")) \
            .where(to_date(lit(PARAM_DATA_DAY), "yyyyMMdd") >= to_date(col("OPEN_DATE"), "yyyyMMddHHmmss"))
        LastMonthEnd = ThisMonth.select(date_sub(to_date(col("OPEN_DATE"), "yyyyMMddHHmmss"), 0).alias('OPEN'))
        T_D_DIM_DIC_BILL_CYCLE = T_D_DIM_DIC_BILL_CYCLE.alias('a').join(LastMonthEnd.alias('b'),
                                                                        to_date(col("a.END_DATE"), "yyyyMMddHHmmss") == to_date(
                                                                            col('b.OPEN'), 'yyyy-MM-dd')).drop('OPEN')

        #date_object = datetime.strptime(param_data_day, "%Y%m%d")
        temp_cycle = T_D_DIM_DIC_BILL_CYCLE.collect()[0]
        
        str_bill_cycle = temp_cycle.BILL_CYCLE_ID
        open_date = temp_cycle.OPEN_DATE
        end_date = temp_cycle.END_DATE
        
        
        --------------
        H03111112 = spark.read \
            .option("header", True) \
            .parquet(
            "hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE_DETAIL/" + param_data_day + "/**/**")\
            .where(
            ((to_timestamp(lit(open_date), 'yyyyMMddHHmmss') <= from_utc_timestamp(to_timestamp("entry_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') ) 
            &
            (from_utc_timestamp(to_timestamp("entry_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') <= to_timestamp(lit(end_date), 'yyyyMMddHHmmss') ) ) 
            | 
            ((to_timestamp(lit(open_date), 'yyyyMMddHHmmss') <= from_utc_timestamp(to_timestamp("last_update_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') ) 
            &
            (from_utc_timestamp(to_timestamp("last_update_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') <= to_timestamp(lit(end_date), 'yyyyMMddHHmmss') ) )
            )
        --------------
        H031 = spark.read \
            .option("header", True) \
            .parquet(
            "hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE_DETAIL/" + param_data_day + "/**/**")\
            .where(
            (
            (to_timestamp(lit(open_date), 'yyyyMMddHHmmss') <= from_utc_timestamp(to_timestamp("entry_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') ) |
            (to_timestamp(lit(open_date), 'yyyyMMddHHmmss') <= from_utc_timestamp(to_timestamp("last_update_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') )  
            )
            &
            (
            (from_utc_timestamp(to_timestamp("entry_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') <= to_timestamp(lit(end_date), 'yyyyMMddHHmmss') ) |
            (from_utc_timestamp(to_timestamp("last_update_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') <= to_timestamp(lit(end_date), 'yyyyMMddHHmmss') )
            )
            )\
            .withColumn("USAGE_SERVICETYPE",
                        when(col("CHARGE_CODE_ID") == '98000501', lit('1523'))
                        .when(col("CHARGE_CODE_ID") == '98000200', lit('1522'))
                        .when(col("CHARGE_CODE_ID") == '98000201', lit('1522'))
                        .when(col("CHARGE_CODE_ID") == '800002', lit('800002'))
                        .when(col("CHARGE_CODE_ID") == '5', lit('5'))
                        .when(col("CHARGE_CODE_ID") == '62', lit('62'))
                        .when(col("CHARGE_CODE_ID") == '1190', lit('1190'))
                        .otherwise(get_json_object(col("EXT_PROPERTY"), '$.USAGE_SERVICETYPE')))\
            .select('ACCT_ID', 'invoice_detail_id', 'bill_cycle_ID', col('open_amt').alias('open_fee'), 'invoice_detail_status', 'USAGE_SERVICETYPE')
            

        H027 = H031.where(col('INVOICE_DETAIL_STATUS') != 'C') \
            .select('ACCT_ID', 'BILL_CYCLE_ID', 'OPEN_FEE', 'USAGE_SERVICETYPE') \
            .withColumn('TRANS_AMT', lit(0)) \
            .withColumn('PAID_FLAG', lit('N'))

        ar_apply_detail_month = ar_apply_detail.alias('a') \
            .join(T_D_DIM_DIC_BILL_CYCLE.alias('b'),
            ((to_timestamp(col('b.open_date'), 'yyyyMMddHHmmss') <= from_utc_timestamp(to_timestamp("a.entry_date",'yyyy-MM-dd HH:mm:ss'),'+3:30')) &
            (from_utc_timestamp(to_timestamp("a.entry_date",'yyyy-MM-dd HH:mm:ss'),'+3:30') < to_timestamp(col('b.end_date'), 'yyyyMMddHHmmss'))))


        ar_apply_detail = spark.read \
            .option("header", True) \
            .parquet(
            "hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/AR_APPLY_DETAIL/" + param_data_day + "/**/**")

        ar_credit_balance = spark.read \
            .option("header", True) \
            .parquet(
            "hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/AR_CREDIT_BALANCE/" + param_data_day + "/**/**") \
            .where(col('CREDIT_TYPE') == 'P')

        step_1 = ar_apply_detail.alias('a').join(ar_credit_balance.alias('b'), col('a.CREDIT_ID') == col('b.CREDIT_ID'))\
                                .drop(col('b.CREDIT_ID'))\
                                .drop(col('b.acct_id'))

        H022 = step_1.alias('a').join(H031.alias('b'), col('a.INVOICE_DETAIL_ID') == col('b.INVOICE_DETAIL_ID'),
                                      'left_outer') \
            .select('CREDIT_ID', 'TRANS_AMT', col('a.ACCT_ID'), 'BILL_CYCLE_ID', 'OPEN_FEE', 'USAGE_SERVICETYPE') \
            .withColumn('PAID_FLAG', lit('Y'))

        H026 = H022.select('TRANS_AMT', 'ACCT_ID', 'BILL_CYCLE_ID', 'OPEN_FEE', 'USAGE_SERVICETYPE',
                           'PAID_FLAG') \
            .unionAll(
            H027.select('TRANS_AMT', 'ACCT_ID', 'BILL_CYCLE_ID', 'OPEN_FEE', 'USAGE_SERVICETYPE',
                        'PAID_FLAG')
        )
        H026 = H026.withColumn('CHARGE_FEE', col('TRANS_AMT')).drop(col('TRANS_AMT'))

        inf_account = spark.read \
            .option("header", True) \
            .parquet("hdfs://" + param_hdfs_addr + "/user/bdi/CRM/INPUT_DATA/INF_ACCOUNT/" + param_data_day + "/**/**") \
            .withColumn('province_id', when((col("C_EX_FIELD4") == lit("0")) |
                                            (col("C_EX_FIELD4").isNull()),
                                            lit("4380")).otherwise(col("C_EX_FIELD4"))) \
            .select(col('acct_id'), 'province_id')

        bc_acct = spark.read \
            .option("header", True) \
            .parquet("hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/BC_ACCT/" + param_data_day + "/**/**") \
            .select('TP_ACCT_KEY', col('ACCT_ID').alias('cbs_acct_id'), 'PAYMENT_TYPE')

        temp = bc_acct.alias('a') \
            .join(inf_account.alias('b'),
                  col('a.TP_ACCT_KEY').cast('string') == col('b.acct_id').cast('string')) \
            .where(col('a.PAYMENT_TYPE') == 1)
        step2 = H026.alias('a').join(temp.alias('b'), col('a.ACCT_ID') == col('b.cbs_acct_id'), 'left_outer')

        t_l_rpt_calcul_settle_debt_2_m = step2.groupBy(
            col("BILL_CYCLE_ID"),
            col("USAGE_SERVICETYPE"),
            col("PAID_FLAG"),
            col("PROVINCE_ID")
        ) \
            .agg(sum(col('CHARGE_FEE')).alias("CHARGE_FEE"),
                 sum(col('OPEN_FEE')).alias("OPEN_FEE")) \
            .withColumn('DATA_MONTH', lit(param_data_month))

        ar_invoice_detail = spark.read \
            .option("header", True) \
            .parquet(
            "hdfs://" + param_hdfs_addr + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE_DETAIL/" + param_data_day + "/**/**")

        H018 = ar_invoice_detail \
            .where(col('INVOICE_DETAIL_STATUS') != 'C') \
            .select('ACCT_ID', 'TRANS_TYPE', col('OPEN_AMT').alias('OPEN_FEE'), col('TAX_AMT').alias('TAX_FEE'), col('CHARGE_AMT').alias('INVOICE_FEE'), 'BILL_CYCLE_ID') \
            .withColumn('TRANS_AMT', lit(0)) \
            .withColumn('PAID_FLAG', lit('N'))

        step_1 = ar_apply_detail.alias('a').join(ar_credit_balance.alias('b'), col('a.CREDIT_ID') == col('b.CREDIT_ID'))\
                                .drop(col('b.CREDIT_ID'))\
                                .drop(col('b.acct_id'))\
                                .drop(col('b.TAX_AMT'))

        H021 = step_1.alias('a').join(ar_invoice_detail.alias('b'),
                                      col('a.INVOICE_DETAIL_ID') == col('b.INVOICE_DETAIL_ID'),
                                      'left_outer') \
            .select('a.ACCT_ID', 'TRANS_TYPE', col('OPEN_AMT').alias('OPEN_FEE'), col('a.TAX_AMT').alias('TAX_FEE'), col('CHARGE_AMT').alias('INVOICE_FEE'), 'BILL_CYCLE_ID', 'TRANS_AMT') \
            .withColumn('PAID_FLAG', lit('Y'))

        H016 = H018.select('ACCT_ID', 'TRANS_TYPE', 'BILL_CYCLE_ID', 'INVOICE_FEE', 'OPEN_FEE', 'TAX_FEE', 'TRANS_AMT',
                           'PAID_FLAG') \
            .unionAll(
            H021.select('ACCT_ID', 'TRANS_TYPE', 'BILL_CYCLE_ID', 'INVOICE_FEE', 'OPEN_FEE', 'TAX_FEE', 'TRANS_AMT',
                        'PAID_FLAG')
        )

        step2_1 = H016.alias('a').join(temp.alias('b'), col('a.acct_id') == col('b.cbs_acct_id'), 'left_outer')

        t_l_rpt_calcul_settle_debt_m = step2_1.groupBy(
            col("BILL_CYCLE_ID"),
            col("PAID_FLAG"),
            col("PROVINCE_ID")
        ) \
            .agg(sum(col('INVOICE_FEE')).alias("INVOICE_FEE"),
                 sum(col('OPEN_FEE')).alias("OPEN_FEE"),
                 sum(col('TAX_FEE')).alias("TAX_FEE"),
                 sum(col('TRANS_AMT')).alias("TRANS_FEE")
                 ) \
            .withColumn('DATA_MONTH', lit(param_data_month)) \
            .withColumn('USAGE_SERVICETYPE', lit(9000))

        final_step_1 = t_l_rpt_calcul_settle_debt_m \
            .where(col('DATA_MONTH') == lit(param_data_month)) \
            .where(col('USAGE_SERVICETYPE').isNotNull()) \
            .groupBy(
            col('usage_servicetype').alias('usage_service_type'),
            col('bill_cycle_ID').alias('bill_cycle_id'),
            col('province_id').alias('province_id')) \
            .agg(
            sum(
                when(
                    (
                            (col("bill_cycle_id") == str_bill_cycle) &
                            (col('paid_flag') == 'Y')
                    )
                    , (abs(coalesce(col('trans_fee'), lit(0))) / coalesce(col('invoice_fee'), lit(1))) * coalesce(
                        col('tax_fee'), lit(1))).otherwise(lit(0))
            ).alias('paid_amt'),
            sum(
                when(
                    (
                            (col("bill_cycle_id") < str_bill_cycle) &
                            (col('paid_flag') == 'N')
                    )
                    , (coalesce(col('open_fee'), lit(0)) / coalesce(col('invoice_fee'), lit(1))) * coalesce(
                        col('tax_fee'), lit(1))).otherwise(lit(0))
            ).alias('amt_previcycle'),
            sum(
                when(
                    (
                            (col("bill_cycle_id") == str_bill_cycle) &
                            (col('paid_flag') == 'N')
                    )
                    , (coalesce(col('open_fee'), lit(0)) / coalesce(col('invoice_fee'), lit(1))) * coalesce(
                        col('tax_fee'), lit(1))).otherwise(lit(0))
            ).alias('amt_currentcycle'),
            (
                    sum(
                        when(
                            (
                                    (col("bill_cycle_id") == str_bill_cycle) &
                                    (col('paid_flag') == 'Y')
                            )
                            ,
                            abs((coalesce(col('trans_fee'), lit(0))) / coalesce(col('invoice_fee'), lit(1))) * coalesce(
                                col('tax_fee'), lit(1))).otherwise(lit(0))
                    ) + sum(
                when((col('paid_flag') == 'N')
                     , (coalesce(col('open_fee'), lit(0)) / coalesce(col('invoice_fee'), lit(1))) * coalesce(
                        col('tax_fee'), lit(1))).otherwise(lit(0)))
            ).alias('amt_speciservice'),
            abs(sum(
                when(
                    (
                            (col("bill_cycle_id") < str_bill_cycle) &
                            (col('paid_flag') == 'Y')
                    )
                    , (abs(coalesce(col('trans_fee'), lit(0))) / coalesce(col('invoice_fee'), lit(1))) * coalesce(
                        col('tax_fee'), lit(1))).otherwise(lit(0))
            )).alias('partpymt_precycle'),

            abs(sum(
                when((col('paid_flag') == 'Y')
                     , (abs(coalesce(col('trans_fee'), lit(0))) / coalesce(col('invoice_fee'), lit(1))) * coalesce(
                        col('tax_fee'), lit(1))).otherwise(lit(0))
            )).alias('partpymt_currentcycle')
        ).withColumn('DATA_MONTH', lit(param_data_month))

        final_step_2 = t_l_rpt_calcul_settle_debt_2_m \
            .where(col('DATA_MONTH') == lit(param_data_month)) \
            .where(col('USAGE_SERVICETYPE').isNotNull()) \
            .groupBy(
            col('usage_servicetype').alias('usage_service_type'),
            col('bill_cycle_ID').alias('bill_cycle_id'),
            col('province_id').alias('province_id')) \
            .agg(
            sum(
                when(
                    (
                            (col("bill_cycle_id") == str_bill_cycle) &
                            (col('paid_flag') == 'Y')
                    )
                    , (abs(coalesce(col('charge_fee'), lit(0))))).otherwise(lit(0))).alias('paid_amt'),
            sum(
                when(
                    (
                            (col("bill_cycle_id") < str_bill_cycle) &
                            (col('paid_flag') == 'N')
                    )
                    , coalesce(col('open_fee'), lit(0))).otherwise(lit(0))
            ).alias('amt_previcycle'),
            sum(
                when(
                    (
                            (col("bill_cycle_id") == str_bill_cycle) &
                            (col('paid_flag') == 'N')
                    )
                    , coalesce(col('open_fee'), lit(0))).otherwise(lit(0))
                       ).alias('amt_currentcycle'),
            (
                    sum(
                        when(
                            (
                                    (col("bill_cycle_id") == str_bill_cycle) &
                                    (col('paid_flag') == 'Y')
                            )
                            , abs(coalesce(col('charge_fee'), lit(0)))).otherwise(lit(0))
                        ) + sum(
                when(col('paid_flag') == 'N'
                     , coalesce(col('open_fee'), lit(0))).otherwise(lit(0)))
            ).alias('amt_speciservice'),
            abs(sum(
                when(
                    (
                            (col("bill_cycle_id") < str_bill_cycle) &
                            (col('paid_flag') == 'Y')
                    )
                    , abs(coalesce(col('charge_fee'), lit(0)))).otherwise(lit(0))
                )).alias('partpymt_precycle'),
            abs(sum(
                when(
                    (
                            (col("bill_cycle_id") == str_bill_cycle) &
                            (col('paid_flag') == 'Y')
                    )
                    , abs(coalesce(col('charge_fee'), lit(0)))
                ).otherwise(lit(0)))).alias('partpymt_currentcycle')
        )\
        .withColumn('DATA_MONTH', lit(param_data_month))

        t_m_rpt_calcul_settle_debt_m = final_step_1.unionAll(final_step_2)

        ######################### Ui
        sys_region = spark.read \
            .option("header", True) \
            .parquet("hdfs://" + param_hdfs_addr + "/user/bdi/CRM/INTERMEDIATE/MV_REGION/" + param_data_day + '/**')

        final_ui_1 = t_m_rpt_calcul_settle_debt_m.alias('a').join(sys_region.alias('b'),
                                                                col('a.province_id') == col('b.region_id'), 'left_outer')

        dim = spark.read.option('header', True)\
                   .csv("hdfs://" + param_hdfs_addr + "/user/bdi/dimensions/dim_usage_serv_type.csv")

        final_ui_2 = final_ui_1.alias('a').join(dim.alias('b'),
                                                    col('a.usage_service_type') == col('b.type_id'), 'left_outer')
        final_ui = final_ui_2.select(
            col('data_month').alias('DATA MONTH'),
            col('region_name').alias('PROVINCE'),
            col('bill_cycle_id').alias('CYCLE ID'),
            col('type_name').alias('INVOICE ITEMS'),
            col('paid_amt').alias('PAID AMOUNT'),
            col('amt_previcycle').alias('AMOUNT FOR PREVIOUS CYCLE'),
            col('amt_currentcycle').alias('AMOUNT FOR CURRENT CYCLE'),
            col('amt_speciservice').alias('AMOUNT FOR SPECIFIC SERVICE'),
            col('partpymt_precycle').alias('PAYMENT FOR PREVIOUS CYCLE'),
            col('partpymt_currentcycle').alias('PAYMENT FOR CURRENT CYCLE'),
        )
        final_ui\
            .write\
            .format('jdbc')\
            .mode('overwrite')\
            .option('url','jdbc:oracle:thin:@//'+param_oracle_addr)\
            .option('driver','oracle.jdbc.driver.OracleDriver')\
            .option('dbtable', 'debt_condition')\
            .option('user',param_oracle_user)\
            .option('password', param_oracle_pass)\
            .save()




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('arguments', nargs='+',
                        help='an integer for the accumulator')
    args = parser.parse_args()
    sys_arg = vars(args).get('arguments')
    param_data_day_int = sys_arg[0]
    param_master_int = sys_arg[1]
    param_hdfs_addr_int = sys_arg[2]
    param_oracle_addr=sys_arg[3]
    param_oracle_user=sys_arg[4]
    param_oracle_pass=sys_arg[5]
    instance = Debt(param_data_day_int, param_master_int, param_hdfs_addr_int, param_oracle_addr, param_oracle_user, param_oracle_pass)
    instance.run_spark()
