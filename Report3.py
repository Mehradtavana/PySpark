from pyspark.sql import SparkSession, Row
import argparse
import os
from persiantools.jdatetime import JalaliDateTime
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, to_timestamp, col, expr, when, expr , to_date,date_sub, translate, get_json_object,coalesce, abs, add_months, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/bigdata/ojdbc7.jar,/temp-spark/udf/Persian_Date-1.0-SNAPSHOT-jar-with-dependencies.jar pyspark-shell'
from datetime import timedelta, datetime

parser = argparse.ArgumentParser()
parser.add_argument('arguments', nargs='+',help='an integer for the accumulator')
args = parser.parse_args()
sys_arg = vars(args).get('arguments')
PARAM_DATA_DAY = sys_arg[0] #20230323
BILL_CYCLE_ID = sys_arg[1] #20230201
PARAM_DATA_LAST_MONTH = sys_arg[2] 
PARAM_DATA_MONTH = sys_arg[3] #202302
param_master = sys_arg[4]
PARAM_HDFS_ADDR = sys_arg[5]
PARAM_ORACLE_ADDR = sys_arg[6]
PARAM_ORACLE_USER = sys_arg[7]
PARAM_ORACLE_PASS = sys_arg[8]
PARAM_ReportTableName = sys_arg[9]

spark = SparkSession.builder.appName('CRM_sanavat_375').master(param_master).getOrCreate()

spark.udf.registerJavaFunction("persian_date", "Persian_Date", StringType())
ar_invoice_BLL = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/AR_INVOICE/" + \
            PARAM_DATA_DAY + "/**").where(col("TRANS_TYPE")==lit("BLL"))

AR_APPLY_DETAIL = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/AR_APPLY_DETAIL/" + \
            PARAM_DATA_DAY + "/**")

bc_account = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INPUT_DATA/BC_ACCT/" + \
            PARAM_DATA_DAY + "/**")

inf_account = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/INF_ACCOUNT/" + \
            PARAM_DATA_DAY + "/**")

T_D_DIM_DIC_BILL_CYCLE = spark.read\
            .option("header", True)\
            .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/dimensions/T_D_DIM_DIC_BILL_CYCLE.csv")

inf_customer = spark.read\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/INPUT_DATA/INF_CUSTOMER/" + \
            PARAM_DATA_DAY + "/**")

ar_invoice_spec = spark.read\
        .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CBS/INTERMEDIATE/INCOME_ACCT_PROVINCE/" + \
            PARAM_DATA_MONTH + "/**").select(
            col("bill_cycle_id"), 
            col("acct_id").alias("CBS_ACCT_ID"), 
            col("old_debt_fee"),
            col("DUNNING_FLAG"),
            col("REGION_NAME"),
            col("GROUP_TYPE"),
            col("crm_party_id").alias ("PARTY_ID"),
            col("Invoice_type"),
            col("REGION_NAME")
        ).groupBy ("CBS_ACCT_ID", "bill_cycle_id", "DUNNING_FLAG", "REGION_NAME", "GROUP_TYPE", "PARTY_ID", "Invoice_type","REGION_NAME")\
        .agg(F.sum("old_debt_fee").alias("old_debt_fee"))

t_m_rpt_subs_debt_03 = ar_invoice_spec.select(
        col("CBS_ACCT_ID").alias("ACCT_ID"),
        col("bill_cycle_id").alias("BILL_CYCLE_ID"),
        col("old_debt_fee").alias("TOTAL_OLD_DEBTS"),
        col("GROUP_TYPE"),
        col("PARTY_ID"),
        col("DUNNING_FLAG"),
        col("REGION_NAME")
).withColumn("C_EX_FIELD3", coalesce(col("GROUP_TYPE"), lit(0)))\
.drop("GROUP_TYPE")\
.withColumn("TOTAL_OLD_DEBTS_NVL", coalesce(col("TOTAL_OLD_DEBTS"), lit(0)))\
    .withColumn("BILL_CYCLE_ID", \
        when((col("BILL_CYCLE_ID").isNull()), lit(BILL_CYCLE_ID)).otherwise(col("BILL_CYCLE_ID")))

t_m_rpt_subs_debt_04 = t_m_rpt_subs_debt_03.alias("t").join(inf_customer.alias("tt"),\
    col("t.PARTY_ID") == col("tt.PARTY_ID"), 'left_outer')\
    .where(col("t.C_EX_FIELD3") == lit('0'))\
    .select(
        col("t.REGION_NAME"),
        col("t.C_EX_FIELD3"),
        col("t.BILL_CYCLE_ID"),
        col("t.TOTAL_OLD_DEBTS_NVL"),
        col("tt.CUST_TYPE")
    )

t_m_rpt_subs_debt_041_in = t_m_rpt_subs_debt_04.where(col("CUST_TYPE") == lit('1'))\
    .select(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("TOTAL_OLD_DEBTS_NVL")
    ).groupBy(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME")
    ).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("NORMAL_DEBT_FEE"))
    
t_m_rpt_subs_debt_041 = t_m_rpt_subs_debt_041_in.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    col("NORMAL_DEBT_FEE"),
    lit(0).alias("CORP_DEBT_FEE"),
    lit(0).alias("TEST_DEBT_FEE"),
    lit(0).alias("PUBLIC_DEBT_FEE"),
    lit(0).alias("RURAL_DEBT_FEE"),
    lit(0).alias("GROUP_DEBT_FEE"),
    lit(0).alias("RED_DEBT_FEE")
)

t_m_rpt_subs_debt_042_in = t_m_rpt_subs_debt_04.where((col("CUST_TYPE") != lit('1')) & (col("CUST_TYPE").isNull()))\
    .select(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("TOTAL_OLD_DEBTS_NVL")
    ).groupBy(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME")
    ).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("CORP_DEBT_FEE"))

t_m_rpt_subs_debt_042 = t_m_rpt_subs_debt_042_in.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    lit(0).alias("NORMAL_DEBT_FEE"),
    col("CORP_DEBT_FEE"),
    lit(0).alias("TEST_DEBT_FEE"),
    lit(0).alias("PUBLIC_DEBT_FEE"),
    lit(0).alias("RURAL_DEBT_FEE"),
    lit(0).alias("GROUP_DEBT_FEE"),
    lit(0).alias("RED_DEBT_FEE")
)

t_m_rpt_subs_debt_031_in = t_m_rpt_subs_debt_03.where(col("C_EX_FIELD3").isin('6047','6040'))\
    .select(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("TOTAL_OLD_DEBTS_NVL")
    ).groupBy(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME")
    ).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("TEST_DEBT_FEE"))

t_m_rpt_subs_debt_031 = t_m_rpt_subs_debt_031_in.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    lit(0).alias("NORMAL_DEBT_FEE"),
    lit(0).alias("CORP_DEBT_FEE"),
    col("TEST_DEBT_FEE"),
    lit(0).alias("PUBLIC_DEBT_FEE"),
    lit(0).alias("RURAL_DEBT_FEE"),
    lit(0).alias("GROUP_DEBT_FEE"),
    lit(0).alias("RED_DEBT_FEE")
)

t_m_rpt_subs_debt_032_in = t_m_rpt_subs_debt_03.where(col("C_EX_FIELD3").isin('6046'))\
    .select(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("TOTAL_OLD_DEBTS_NVL")
    ).groupBy(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME")
    ).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("PUBLIC_DEBT_FEE"))

t_m_rpt_subs_debt_032 = t_m_rpt_subs_debt_032_in.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    lit(0).alias("NORMAL_DEBT_FEE"),
    lit(0).alias("CORP_DEBT_FEE"),
    lit(0).alias("TEST_DEBT_FEE"),
    col("PUBLIC_DEBT_FEE"),
    lit(0).alias("RURAL_DEBT_FEE"),
    lit(0).alias("GROUP_DEBT_FEE"),
    lit(0).alias("RED_DEBT_FEE")
)

t_m_rpt_subs_debt_033_in = t_m_rpt_subs_debt_03.where(col("C_EX_FIELD3").isin('6010'))\
    .select(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("TOTAL_OLD_DEBTS_NVL")
    ).groupBy(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME")
    ).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("RURAL_DEBT_FEE"))

t_m_rpt_subs_debt_033 = t_m_rpt_subs_debt_033_in.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    lit(0).alias("NORMAL_DEBT_FEE"),
    lit(0).alias("CORP_DEBT_FEE"),
    lit(0).alias("TEST_DEBT_FEE"),
    lit(0).alias("PUBLIC_DEBT_FEE"),
    col("RURAL_DEBT_FEE"),
    lit(0).alias("GROUP_DEBT_FEE"),
    lit(0).alias("RED_DEBT_FEE")
)

xx = t_m_rpt_subs_debt_03.where((col("C_EX_FIELD3") > lit('0')) & (col("C_EX_FIELD3") < lit('1000')))\
    .select(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("TOTAL_OLD_DEBTS_NVL")
    )
t_m_rpt_subs_debt_034_in = xx.groupBy(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME")
    ).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("GROUP_DEBT_FEE"))

t_m_rpt_subs_debt_034 = t_m_rpt_subs_debt_034_in.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    lit(0).alias("NORMAL_DEBT_FEE"),
    lit(0).alias("CORP_DEBT_FEE"),
    lit(0).alias("TEST_DEBT_FEE"),
    lit(0).alias("PUBLIC_DEBT_FEE"),
    lit(0).alias("RURAL_DEBT_FEE"),
    col("GROUP_DEBT_FEE"),
    lit(0).alias("RED_DEBT_FEE")
)

t_m_rpt_subs_debt_035_in = t_m_rpt_subs_debt_03.where((col("C_EX_FIELD3")==lit('6102')) & (col("DUNNING_FLAG") == lit('3')))\
    .select(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("TOTAL_OLD_DEBTS_NVL")
    ).groupBy(
        col("BILL_CYCLE_ID"),
        col("REGION_NAME")
    ).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("RED_DEBT_FEE"))

t_m_rpt_subs_debt_035 = t_m_rpt_subs_debt_035_in.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    lit(0).alias("NORMAL_DEBT_FEE"),
    lit(0).alias("CORP_DEBT_FEE"),
    lit(0).alias("TEST_DEBT_FEE"),
    lit(0).alias("PUBLIC_DEBT_FEE"),
    lit(0).alias("RURAL_DEBT_FEE"),
    lit(0).alias("GROUP_DEBT_FEE"),
    col("RED_DEBT_FEE")
)

u1 = t_m_rpt_subs_debt_041.select(col("BILL_CYCLE_ID"), col("REGION_NAME"), col("NORMAL_DEBT_FEE"), col("CORP_DEBT_FEE"), col("TEST_DEBT_FEE"), col("PUBLIC_DEBT_FEE"), col("RURAL_DEBT_FEE"), col("GROUP_DEBT_FEE"), col("RED_DEBT_FEE"))
u2 = t_m_rpt_subs_debt_042.select(col("BILL_CYCLE_ID"), col("REGION_NAME"), col("NORMAL_DEBT_FEE"), col("CORP_DEBT_FEE"), col("TEST_DEBT_FEE"), col("PUBLIC_DEBT_FEE"), col("RURAL_DEBT_FEE"), col("GROUP_DEBT_FEE"), col("RED_DEBT_FEE"))
u3 = t_m_rpt_subs_debt_031.select(col("BILL_CYCLE_ID"), col("REGION_NAME"), col("NORMAL_DEBT_FEE"), col("CORP_DEBT_FEE"), col("TEST_DEBT_FEE"), col("PUBLIC_DEBT_FEE"), col("RURAL_DEBT_FEE"), col("GROUP_DEBT_FEE"), col("RED_DEBT_FEE"))
u4 = t_m_rpt_subs_debt_032.select(col("BILL_CYCLE_ID"), col("REGION_NAME"), col("NORMAL_DEBT_FEE"), col("CORP_DEBT_FEE"), col("TEST_DEBT_FEE"), col("PUBLIC_DEBT_FEE"), col("RURAL_DEBT_FEE"), col("GROUP_DEBT_FEE"), col("RED_DEBT_FEE"))
u5 = t_m_rpt_subs_debt_033.select(col("BILL_CYCLE_ID"), col("REGION_NAME"), col("NORMAL_DEBT_FEE"), col("CORP_DEBT_FEE"), col("TEST_DEBT_FEE"), col("PUBLIC_DEBT_FEE"), col("RURAL_DEBT_FEE"), col("GROUP_DEBT_FEE"), col("RED_DEBT_FEE"))
u6 = t_m_rpt_subs_debt_034.select(col("BILL_CYCLE_ID"), col("REGION_NAME"), col("NORMAL_DEBT_FEE"), col("CORP_DEBT_FEE"), col("TEST_DEBT_FEE"), col("PUBLIC_DEBT_FEE"), col("RURAL_DEBT_FEE"), col("GROUP_DEBT_FEE"), col("RED_DEBT_FEE"))
u7 = t_m_rpt_subs_debt_035.select(col("BILL_CYCLE_ID"), col("REGION_NAME"), col("NORMAL_DEBT_FEE"), col("CORP_DEBT_FEE"), col("TEST_DEBT_FEE"), col("PUBLIC_DEBT_FEE"), col("RURAL_DEBT_FEE"), col("GROUP_DEBT_FEE"), col("RED_DEBT_FEE"))

t_m_rpt_subs_debt_05_union = u1.union(u2).union(u3).union(u4).union(u5).union(u6).union(u7)

t_m_rpt_subs_debt_05 = t_m_rpt_subs_debt_05_union.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    col("NORMAL_DEBT_FEE"),
    col("CORP_DEBT_FEE"),
    col("TEST_DEBT_FEE"),
    col("PUBLIC_DEBT_FEE"),
    col("RURAL_DEBT_FEE"),
    col("GROUP_DEBT_FEE"),
    col("RED_DEBT_FEE")
).groupBy(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME")
).agg(
    F.sum(col("NORMAL_DEBT_FEE")).alias("NORMAL_DEBT_FEE"),
    F.sum(col("CORP_DEBT_FEE")).alias("CORP_DEBT_FEE"),
    F.sum(col("TEST_DEBT_FEE")).alias("TEST_DEBT_FEE"),
    F.sum(col("PUBLIC_DEBT_FEE")).alias("PUBLIC_DEBT_FEE"),
    F.sum(col("RURAL_DEBT_FEE")).alias("RURAL_DEBT_FEE"),
    F.sum(col("GROUP_DEBT_FEE")).alias("GROUP_DEBT_FEE"),
    F.sum(col("RED_DEBT_FEE")).alias("RED_DEBT_FEE")
)

t_m_rpt_subs_debt_06_j1 = t_m_rpt_subs_debt_03.select(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME"),
    col("TOTAL_OLD_DEBTS_NVL")
).groupBy(
    col("BILL_CYCLE_ID"),
    col("REGION_NAME")
).agg(F.sum(col("TOTAL_OLD_DEBTS_NVL")).alias("TOTAL_DEBT_FEE1"))

conditions = [(col("t.BILL_CYCLE_ID") == col("tt.BILL_CYCLE_ID")) & (col("t.REGION_NAME") == col("tt.REGION_NAME"))]

t_m_rpt_subs_debt_06 = t_m_rpt_subs_debt_05.alias("t").join(t_m_rpt_subs_debt_06_j1.alias("tt"), conditions, 'left_outer')\
    .select(
        col("t.BILL_CYCLE_ID"),
        col("t.REGION_NAME"),
        col("t.NORMAL_DEBT_FEE"),
        col("t.CORP_DEBT_FEE"),
        col("t.TEST_DEBT_FEE"),
        col("t.PUBLIC_DEBT_FEE"),
        col("t.RURAL_DEBT_FEE"),
        col("t.GROUP_DEBT_FEE"),
        col("t.RED_DEBT_FEE"),
        col("tt.TOTAL_DEBT_FEE1")
    ).where((col("t.REGION_NAME") == col("tt.REGION_NAME")))
'''
###
T_M_RPT_SUBS_DEBT_M = spark.read\
            .option("header", True)\
            .parquet("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/CRM/RESULT_DATA/Sanavat/" + PARAM_DATA_LAST_MONTH + "/**")
'''
T_M_RPT_SUBS_DEBT_M = spark.read\
            .option("header", True)\
            .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/first_run/t_m_rpt_subs_debt_m_202301_20230404114010.csv")\
            .select(col("*"), col("PROVINCE_ID").alias("REGION_NAME")).drop("PROVINCE_ID")

t_m_rpt_subs_debt_07 = t_m_rpt_subs_debt_06.select(col("BILL_CYCLE_ID"),col("REGION_NAME"),col("NORMAL_DEBT_FEE"),col("CORP_DEBT_FEE"),col("TEST_DEBT_FEE"),col("PUBLIC_DEBT_FEE"),col("RURAL_DEBT_FEE"),col("GROUP_DEBT_FEE"),col("RED_DEBT_FEE"),col("TOTAL_DEBT_FEE1"))\
    .unionAll(
        T_M_RPT_SUBS_DEBT_M.select(col("BILL_CYCLE_ID"),col("REGION_NAME"),col("NORMAL_DEBT_FEE"),col("CORP_DEBT_FEE"),col("TEST_DEBT_FEE"),col("PUBLIC_DEBT_FEE"),col("RURAL_DEBT_FEE"),col("GROUP_DEBT_FEE"),col("RED_DEBT_FEE"),col("TOTAL_DEBT_FEE"))
    )

cond = [(col("a.ENTRY_DATE") >= to_date(col("b.OPEN_DATE")[0:8],'yyyyMMdd')) &\
        (col("a.ENTRY_DATE") < to_date(col("b.END_DATE")[0:8],'yyyyMMdd'))
        ]
t_m_rpt_subs_debt_001 = AR_APPLY_DETAIL.alias("a").join(T_D_DIM_DIC_BILL_CYCLE.alias("b"), cond, 'inner')\
    .select(
        col("a.INVOICE_ID"),
        col("a.TRANS_AMT"),
        col("a.ACCT_ID")
    ).withColumn("TRANS_AMT", abs(col("a.TRANS_AMT")/lit(1000)))\
    .withColumn("month_1", expr("date_format(add_months(to_date('" + lit(PARAM_DATA_DAY) + "','yyyyMMdd'),-1),'yyyyMM')"))\
    .where(col("b.IN_MONTH") >= col("month_1"))
        

t_m_rpt_subs_debt_002_1 = ar_invoice_spec.select(col("CBS_ACCT_ID"), col("PARTY_ID"), col("REGION_NAME"))

t_m_rpt_subs_debt_002_1_a = t_m_rpt_subs_debt_001.alias("t").join(ar_invoice_BLL.alias("tt"), col("t.INVOICE_ID") == col("tt.INVOICE_ID"), 'left_outer')\
    .select(
        col("t.TRANS_AMT"),
        col("tt.bill_cycle_id").alias("BILL_CYCLE_ID"),
        col("t.ACCT_ID"),
        col("INVOICE_ID")
    )

t_m_rpt_subs_debt_002 = t_m_rpt_subs_debt_002_1_a.alias("t1").join(t_m_rpt_subs_debt_002_1.alias("t"), col("t1.ACCT_ID") == col("CBS_ACCT_ID"), 'left_outer')\
    .select(
        col("t1.TRANS_AMT"),
        col("t1.BILL_CYCLE_ID"),
        col("t1.ACCT_ID"),
        col("t.REGION_NAME")
    ).groupBy(
        col("t1.BILL_CYCLE_ID"),
        col("t.REGION_NAME")
    ).agg(F.sum(coalesce(col("t1.TRANS_AMT"), lit(0))).alias("Trans_amt"))
    

condi = [(col("t.BILL_CYCLE_ID") == col("tt.BILL_CYCLE_ID")) & (col("t.REGION_NAME") == col("tt.REGION_NAME"))]
inn = t_m_rpt_subs_debt_07.alias("t").join(t_m_rpt_subs_debt_003.alias("tt"), condi, 'left_outer')\
    .select(
        col("t.BILL_CYCLE_ID"),
        col("t.REGION_NAME"),
        col("t.NORMAL_DEBT_FEE"),
        col("t.CORP_DEBT_FEE"),
        col("t.TEST_DEBT_FEE"),
        col("t.PUBLIC_DEBT_FEE"),
        col("t.RURAL_DEBT_FEE"),
        col("t.GROUP_DEBT_FEE"),
        col("t.total_debt_fee1"),
        col("t.RED_DEBT_FEE"),
        col("tt.Trans_amt1")
    ).withColumn('DATA_MONTH', expr("date_format(add_months(to_date('" + PARAM_DATA_DAY + "','yyyyMMdd'),-1),'yyyyMM')")
    ).withColumn("IRAN_DATE", expr("persian_date("+PARAM_DATA_DAY+",'yyyyMMdd')")
    ).withColumn("PRE_TOTAL_DEBT_FEE", lit(0)
    ).withColumn("PROCS_TIME", expr("to_date('" + PARAM_DATA_DAY + "','yyyyMMdd')")
    ).withColumn("LAST_OPEN_FEE_B", coalesce(col("t.NORMAL_DEBT_FEE"), lit("0"))
    ).withColumn("CORP_DEBT_FEE", coalesce(col("t.CORP_DEBT_FEE"), lit("0"))
    ).withColumn("TEST_DEBT_FEE", coalesce(col("t.TEST_DEBT_FEE"), lit("0"))
    ).withColumn("PUBLIC_DEBT_FEE", coalesce(col("t.PUBLIC_DEBT_FEE"), lit("0"))
    ).withColumn("RURAL_DEBT_FEE", coalesce(col("t.RURAL_DEBT_FEE"), lit("0"))
    ).withColumn("GROUP_DEBT_FEE", coalesce(col("t.GROUP_DEBT_FEE"), lit("0"))
    ).withColumn("RED_DEBT_FEE", coalesce(col("t.RED_DEBT_FEE"), lit("0"))
    ).withColumn("Trans_amt1", coalesce(col("tt.Trans_amt1"), lit("0"))
    ).withColumn("TOTAL_DEBT_FEE", coalesce(col("t.total_debt_fee1"), lit("0")))

t_m_rpt_subs_debt_m = inn\
    .select(
        col("DATA_MONTH"),
        col("BILL_CYCLE_ID"),
        col("REGION_NAME"),
        col("IRAN_DATE"),
        col("NORMAL_DEBT_FEE"),
        col("CORP_DEBT_FEE"),
        col("TEST_DEBT_FEE"),
        col("PUBLIC_DEBT_FEE"),
        col("RURAL_DEBT_FEE"),
        col("GROUP_DEBT_FEE"),
        col("RED_DEBT_FEE"),
        col("Trans_amt1").alias("DEBT_FEE"),
        col("TOTAL_DEBT_FEE"),
        col("PRE_TOTAL_DEBT_FEE"),
        col("PROCS_TIME"),
    ).where(col("BILL_CYCLE_ID").isNotNull())

a = t_m_rpt_subs_debt_m.where(col("REGION_NAME").isin("تهران", "4380"))
a\
    .repartition(1)\
    .write\
    .option("header", True)\
    .mode('overwrite')\
    .csv("hdfs://" + PARAM_HDFS_ADDR + "/user/bdi/REPORT_RESULT/sanavat/" + PARAM_DATA_DAY)
