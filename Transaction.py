# txn = spark.read.csv("C:/Users/sidse/Downloads/big data course/ASSIGNMENTS/projects/Banking P2/txn.csv", inferSchema=True, header =True)

txn = "https://raw.githubusercontent.com/abhilash-1/pyspark-project/main/txn.csv"
txn = pd.read_csv(txn)
txn = spark.createDataFrame(txn)

txn.printSchema()

#COUNT OF TRANSACTION ON EVERY ACCOUNT
txn.groupBy("Account No").count().show()

#Maximum withdrawal amount
txn.groupBy("Account No").max(" WITHDRAWAL AMT ").orderBy("max( WITHDRAWAL AMT )", ascending = False).show()

#MINIMUM WITHDRAWAL AMOUNT OF AN ACCOUNT
txn.groupBy("Account No").min(" WITHDRAWAL AMT ").orderBy("min( WITHDRAWAL AMT )").show()


#MAXIMUM DEPOSIT AMOUNT OF AN ACCOUNT
txn.groupBy("Account No").max(" DEPOSIT AMT ").orderBy("max( DEPOSIT AMT )", ascending = False).show()


#MINIMUM DEPOSIT AMOUNT OF AN ACCOUNT
txn.groupBy("Account No").min(" DEPOSIT AMT ").orderBy("min( DEPOSIT AMT )").show()

#sum of balance in every bank account
txn.groupBy("Account No").sum("BALANCE AMT").show()

#Number of transaction on each date
txn.groupBy("VALUE DATE").count().orderBy("count", ascending = False).show()

#List of customers with withdrawal amount more than 1 lakh
txn.select("Account No","TRANSACTION DETAILS"," WITHDRAWAL AMT ").filter(txn[" WITHDRAWAL AMT "]>100000).show()
