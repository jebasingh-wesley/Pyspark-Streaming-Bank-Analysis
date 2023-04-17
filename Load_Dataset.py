import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import pandas as pd


spark = SparkSession.builder.master("local[1]") \
                    .appName('banking.com') \
                    .getOrCreate()

"LOAN DATASET"

# df = spark.read.csv("C:/Users/sidse/Downloads/big data course/ASSIGNMENTS/projects/Banking P2/loan.csv", inferSchema = True, header = True)

df = "https://raw.githubusercontent.com/abhilash-1/pyspark-project/main/loan.csv"
df = pd.read_csv(df)
df=spark.createDataFrame(df)
# df.limit(10).toPandas()

# df.printSchema()
# df.show(5)
# print(len(df.columns))
# print(df.count())
# df.distinct().count()

"number of loans in each category"
df.groupBy("Loan Category").count().orderBy("count", ascending = False)#.show()


"number of people who have taken more than 1 lack loan"
# print(df.filter(df["Loan Amount"]>"1,00,000").count())

"number of people with income greater than 60000 rupees"
# print(df.filter(df["Income"]>"60000").count())

#number of people with 2 or more returned cheques and income less than 50000
df.filter((df[" Returned Cheque"]>"1") & (df["Income"]<"50000")).count()

#number of people with 2 or more returned cheques and are single
df.filter((df[" Returned Cheque"]>"1") & (df["Marital Status"]<"SINGLE")).count()

#number of people with expenditure over 50000 a month
df.filter((df["Expenditure"]>"50000"))#.show()


