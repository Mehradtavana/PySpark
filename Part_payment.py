from pyspark.sql import SparkSession, Row
import argparse
import os
from persiantools.jdatetime import JalaliDateTime
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, to_timestamp, col, expr, when, expr , to_date,date_sub, row_number, coalesce, greatest, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

parser = argparse.ArgumentParser()
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0] #20221124~14010904   , 20230424                        
PARAM_DATA_LAST_MONTH = sys_arg[1] #202209 , 202303
PARAM_DATA_MONTH = sys_arg[2] #202210  , 202304
PARAM_DATA_DAY_Last_Month = sys_arg[3] #20230323 for ar_invoice previous cycle
param_master = sys_arg[4]  
PARAM_HDFS_ADDR = sys_arg[5]
PARAM_ORACLE_ADDR = sys_arg[6]
PARAM_ORACLE_USER = sys_arg[7]
PARAM_ORACLE_PASS = sys_arg[8]
PARAM_ReportTableName = sys_arg[9]

spark = SparkSession.builder.appName('Partial_Payment_Report_372_7').master(param_master).getOrCreate()

spark.udf.registerJavaFunction("persian_date", "Persian_Date", StringType())

bc_acct = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/BC_ACCT/" + \
            PARAM_DATA_DAY + "/**")

inf_account = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/INF_ACCOUNT/" + \
            PARAM_DATA_DAY + "/**")

cbs_ar_invoice = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE/" + \
            PARAM_DATA_DAY + "/**")
'''         
t_m_rpt_part_payment_d = spark.read\
            .option("header", True)\
            .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/first_run/t_m_rpt_part_payment_d_202303.csv")
'''
t_m_rpt_part_payment_d = spark.read\
            .option("header", True)\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/Partial_Payment_Report/" + PARAM_DATA_LAST_MONTH + "/**")

#previous cycle:
cbs_ar_invoice_PC = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE/" + \
            PARAM_DATA_DAY_Last_Month + "/**")

rpt_part_payment_01 = cbs_ar_invoice.select(col("INVOICE_ID"),
                              col("ACCT_ID"),
                              col("BILL_CYCLE_ID"),
                              col("PRI_IDENTITY"),
                              col("INVOICE_AMT"),
                              col("OPEN_AMT"),
                              (coalesce(col("INVOICE_AMT"),lit(0))/lit(1000)).alias("INVOICE_FEE"),
                              (coalesce(col("OPEN_AMT"),lit(0))/lit(1000)).alias("OPEN_FEE")
                     ).where(
                              (col("INVOICE_TYPE")==lit("5")) \
                             & ((col("AOD_STATUS") != lit("Y")) | (col("AOD_STATUS").isNull()))\
                            ).withColumn("TRANS_AMT",((coalesce(col("INVOICE_AMT"),lit(0))) -\
                                                (coalesce(col("OPEN_AMT"),lit(0))))/lit(1000))

rpt_part_payment_02 = bc_acct.alias("a").join(inf_account.alias("b"), col("b.ACCT_ID").cast("String") == col("a.TP_ACCT_KEY"), 'left_outer')\
    .where(col("b.PAYMENT_TYPE") == lit("1"))\
    .withColumn("Province_id", when(coalesce(col("b.C_EX_FIELD4"),lit("0"))==lit("0"),lit("4380")).otherwise(col("b.C_EX_FIELD4")))\
    .select(col("a.ACCT_ID").alias("CBS_ACCT_ID"), col("b.PAYMENT_TYPE").alias("PAYMENT_MODE"),col("Province_id"))

rpt_part_payment_03 = rpt_part_payment_01.join(rpt_part_payment_02, \
                                 rpt_part_payment_01.ACCT_ID == rpt_part_payment_02.CBS_ACCT_ID, \
                                 'left_outer').select(col("INVOICE_ID"),col("INVOICE_AMT")\
                                                      ,col("OPEN_AMT"),col("BILL_CYCLE_ID"),\
                                                      col("TRANS_AMT"),col("Province_id"))\
                                            .where((col("PAYMENT_MODE") == lit('1'))\
                                                &(col("INVOICE_AMT") != col("OPEN_AMT"))\
                                                &(col("OPEN_AMT") >= lit(1000))
                                            )
#Pevious Cycle
t_m_rpt_part_payment_04 = cbs_ar_invoice_PC.select(
    col("INVOICE_ID")
).where(
    (col("INVOICE_AMT") == col("OPEN_AMT"))\
    & (col("INVOICE_TYPE") == lit('5'))\
    & ((col("AOD_STATUS") != lit("Y")) | (col("AOD_STATUS").isNull()))
    & (col("BILL_CYCLE_ID")[0:6] < lit(PARAM_DATA_MONTH))
)

t_m_rpt_part_payment_05_1 = rpt_part_payment_03.select(
    col("BILL_CYCLE_ID"),
    col("PROVINCE_ID"),
    col("INVOICE_ID"),
    col("TRANS_AMT"),
    col("INVOICE_AMT")
).groupBy(
    col("BILL_CYCLE_ID"),
    col("PROVINCE_ID")
).agg(F.count(col("INVOICE_ID")).alias("PART_CNT"),
    F.sum("TRANS_AMT").alias("PART_PAY_FEE"),
    F.sum("INVOICE_AMT").alias("PART_INV_FEE")
)

cond = [(col("t.BILL_CYCLE_ID") == col("tt.BILL_CYCLE_ID")) & (col("t.PROVINCE_ID") == col("tt.PROVINCE_ID"))]
t_m_rpt_part_payment_05_2 = t_m_rpt_part_payment_05_1.alias("t").join(t_m_rpt_part_payment_d.alias("tt"), cond, 'full_outer')\
    .select(
        col("t.BILL_CYCLE_ID").alias("BILL_CYCLE_ID1"),
        col("t.PROVINCE_ID").alias("PROVINCE_ID1"),
        col("t.PART_CNT").alias("PART_CNT1"),
        col("t.PART_PAY_FEE").alias("PART_PAY_FEE1"),
        col("t.PART_INV_FEE").alias("PART_INV_FEE1"),
        col("tt.BILL_CYCLE_ID"),
        col("tt.PROVINCE_ID"),
        col("tt.PART_CNT"),
        col("tt.PART_PAY_FEE"),
        col("tt.PART_INV_FEE"),
        col("tt.CHANGE_CNT"),
        col("tt.CHANGE_PAY_FEE"),
        col("tt.CHANGE_INV_FEE"),
        col("tt.PAID_FLAG")
    )

t_m_rpt_part_payment_05 = t_m_rpt_part_payment_05_2.select(
    coalesce(col("BILL_CYCLE_ID"), col("BILL_CYCLE_ID1")).alias("BILL_CYCLE_ID"),
    coalesce(col("PROVINCE_ID"), col("PROVINCE_ID1")).alias("PROVINCE_ID"),
    coalesce(col("PART_CNT1"),lit("0")).alias("PART_CNT"),
    coalesce(col("PART_PAY_FEE1"),lit("0")).alias("PART_PAY_FEE"),
    coalesce(col("PART_INV_FEE1"),lit("0")).alias("PART_INV_FEE"),
    lit("0").alias("CHANGE_CNT"),
    lit("0").alias("CHANGE_PAY_FEE"),
    lit("0").alias("CHANGE_INV_FEE"),
    lit('1').alias("PAID_FLAG")
)

t_m_rpt_part_payment_06_1 = rpt_part_payment_03.alias("t").join(t_m_rpt_part_payment_04.alias("tt"), col("tt.INVOICE_ID") == col("t.INVOICE_ID") , 'inner')\
    .select(
        col("t.BILL_CYCLE_ID"),
        col("t.PROVINCE_ID"),
        col("t.INVOICE_ID"),
        col("t.TRANS_AMT"),
        col("t.INVOICE_AMT")
    )

t_m_rpt_part_payment_06 = t_m_rpt_part_payment_06_1.select(
    col("BILL_CYCLE_ID"),
    col("PROVINCE_ID"),
    lit(0).alias("PART_CNT"),
    lit(0).alias("PART_PAY_FEE"),
    lit(0).alias("PART_INV_FEE"),
    lit('1').alias("PAID_FLAG"),
    col("INVOICE_ID"),
    col("TRANS_AMT"),
    col("INVOICE_AMT")
).groupBy(
    col("BILL_CYCLE_ID"),
    col("PROVINCE_ID"),
    col("PART_CNT"),
    col("PART_PAY_FEE"),
    col("PART_INV_FEE"),
    col("PAID_FLAG")
).agg(
    F.count(col("INVOICE_ID")).alias("CHANGE_CNT"),
    F.sum(col("TRANS_AMT")).alias("CHANGE_PAY_FEE"),
    F.sum(col("INVOICE_AMT")).alias("CHANGE_INV_FEE")
)

t_m_rpt_part_payment_07 = t_m_rpt_part_payment_05.select(col("BILL_CYCLE_ID"), col("PROVINCE_ID"), col("PART_CNT"), col("PART_PAY_FEE"), col("PART_INV_FEE"), col("CHANGE_CNT"), col("CHANGE_PAY_FEE"), col("CHANGE_INV_FEE"), col("PAID_FLAG")).unionAll(
    t_m_rpt_part_payment_06.select(col("BILL_CYCLE_ID"), col("PROVINCE_ID"), col("PART_CNT"), col("PART_PAY_FEE"), col("PART_INV_FEE"), col("CHANGE_CNT"), col("CHANGE_PAY_FEE"), col("CHANGE_INV_FEE"), col("PAID_FLAG"))
    )

t_m_rpt_part_payment_d = t_m_rpt_part_payment_07.select(col("BILL_CYCLE_ID"),col("PROVINCE_ID"),col("PAID_FLAG"),\
                                   col("PART_CNT"),col("PART_PAY_FEE"),col("PART_INV_FEE"),\
                                   col("CHANGE_CNT"),col("CHANGE_PAY_FEE"),col("CHANGE_INV_FEE")
                                  )\
    .groupBy(
            col("BILL_CYCLE_ID"),
            col("PROVINCE_ID"),
            col("PAID_FLAG")
        )\
    .agg(F.sum(col("PART_CNT")).alias("PART_CNT"),\
         F.sum(col("PART_PAY_FEE")).alias("PART_PAY_FEE"),\
         F.sum(col("PART_INV_FEE")).alias("PART_INV_FEE"),\
         F.sum(col("CHANGE_CNT")).alias("CHANGE_CNT"),\
         F.sum(col("CHANGE_PAY_FEE")).alias("CHANGE_PAY_FEE"),\
         F.sum(col("CHANGE_INV_FEE")).alias("CHANGE_INV_FEE")
        ).withColumn('PROCESS_TIME',(to_date(lit(PARAM_DATA_DAY),'yyyyMMdd')))\
         .withColumn('DATA_DAY',date_format(expr("persian_date(PROCESS_TIME,'yyyyMMdd')"),'yyyyMMdd'))

        
MV_REGION = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INTERMEDIATE/MV_REGION/" + \
            PARAM_DATA_DAY + "/**")

Res = t_m_rpt_part_payment_d.join(MV_REGION, MV_REGION.region_id == t_m_rpt_part_payment_d.PROVINCE_ID).drop("region_id","PARENT_REGION_ID","CITY_NAME")\
    .withColumn("PARTIAL_PAYMENT_RATIO",when(col("PART_INV_FEE")==lit(0), lit(0)).otherwise((col("PART_PAY_FEE")/col("PART_INV_FEE"))))\
    .withColumn("RATIO",when(col("CHANGE_INV_FEE")==lit(0), lit(0)).otherwise((col("CHANGE_PAY_FEE")/col("CHANGE_INV_FEE"))))

Result = Res.persist()

Result\
    .write\
    .mode('overwrite')\
    .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/Partial_Payment_Report/" + PARAM_DATA_MONTH)

Result\
.write\
.format("jdbc")\
.mode("append")\
.option("driver", "oracle.jdbc.driver.OracleDriver")\
.option("user", PARAM_ORACLE_USER)\
.option("password", PARAM_ORACLE_PASS)\
.option("url", "jdbc:oracle:thin:@//" + PARAM_ORACLE_ADDR)\
.option("dbtable", PARAM_ReportTableName)\
.save()
