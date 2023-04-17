# dfc = spark.read.csv("C:/Users/sidse/Downloads/big data course/ASSIGNMENTS/projects/Banking P2/credit card.csv", inferSchema = True, header = True)

dfc = "https://raw.githubusercontent.com/abhilash-1/pyspark-project/main/credit%20card.csv"
dfc = pd.read_csv(dfc)
dfc=spark.createDataFrame(dfc)

# dfc.printSchema()
# dfc.show(5)

"number of members who are elgible for credit card"
print(dfc.filter(dfc["CreditScore"]>700).count())

#number of members who are  elgible and active in the bank
dfc.filter((dfc["IsActiveMember"]==1) & (dfc["CreditScore"]>700)).count()

#credit card users in Spain
dfc.filter(dfc["Geography"]=="Spain")#.show(10)

dfc.filter((dfc["EstimatedSalary"]>100000) & (dfc["Exited"]==1)).count()

dfc.filter((dfc["EstimatedSalary"]<100000) & (dfc["NumOfProducts"]>1)).count()

