# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sliver Layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access using App

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.prjstorageone.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.prjstorageone.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.prjstorageone.dfs.core.windows.net", "3f5ccf0e-1b99-4c77-a454-ea357d7a5f69")
spark.conf.set("fs.azure.account.oauth2.client.secret.prjstorageone.dfs.core.windows.net", "TRF8Q~QAsy3OfnnHlUKB5RYB1X3_BZtpm49B7av8")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.prjstorageone.dfs.core.windows.net", "https://login.microsoftonline.com/ad7cdf1f-91d8-4574-9280-89d4d999da23/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Data

# COMMAND ----------

df_cal=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cust=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_prodCat=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_prodSubCat=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

# COMMAND ----------

df_prod=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

# MAGIC %md
# MAGIC **AdventureWorks_Sales'*' it is read all the 3 years data at once**

# COMMAND ----------

df_sales=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_terr=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@prjstorageone.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC # TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Calendar

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal=df_cal.withColumn('Month',month(col('Date'))).withColumn('Year',year(col('Date')))
display(df_cal)

# COMMAND ----------

# MAGIC %md
# MAGIC **Saving transformations in Silver layer**

# COMMAND ----------

df_cal.write.format('parquet').mode('append').option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Calendar").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Customers

# COMMAND ----------

df_cust.display()

# COMMAND ----------

# MAGIC %md
# MAGIC There are two ways of merging the string columns this is simple one so we na not saving in our transformation we will use second one 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1

# COMMAND ----------

df_cust.withColumn("FullName",concat(col("Prefix"),lit(' '),col("FirstName"),lit(' '),col("LastName"),lit(' '))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2

# COMMAND ----------

df_cust=df_cust.withColumn("FullName",concat_ws(' ',col("Prefix"),col("FirstName"),("LastName"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Saving the customer tranformation in to silver layer**

# COMMAND ----------

df_cust.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Customer")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.Product_Categories

# COMMAND ----------

# MAGIC %md
# MAGIC **No Transformation Needed**

# COMMAND ----------

df_prodCat.display()

# COMMAND ----------

df_prodCat.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Product_Categories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.Product_Subcategories

# COMMAND ----------

df_prodSubCat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC No Transformation Needed

# COMMAND ----------

df_prodSubCat.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.Products

# COMMAND ----------

df_prod.display()

# COMMAND ----------

# MAGIC %md
# MAGIC In this transformation we are not creating new column just repalces or modify the existing column 

# COMMAND ----------

df_prod=df_prod.withColumn("ProductSKU",split(col("ProductSKU"),'-')[0])\
                .withColumn("ProductName",split(col("ProductName"),' ')[0]).display()

# COMMAND ----------

df_prod.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Returns

# COMMAND ----------

# MAGIC %md
# MAGIC there is no need to transform 

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.Territories

# COMMAND ----------

df_terr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC no need to transform beacuse there is not much to transform 

# COMMAND ----------

df_terr.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 converting date column into timestamp on this column(StockDate)

# COMMAND ----------

df_sales=df_sales.withColumn("StockDate",to_timestamp("StockDate")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  9.2 on this cloumn (OrderNumber) we will use replace function i.e alphabet S is replacees with alphabet T

# COMMAND ----------

df_sales=df_sales.withColumn("OrderNumber",regexp_replace(col("OrderNumber"),'S','T')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 Performing Arithmatic operations 

# COMMAND ----------

df_sales=df_sales.withColumn("Multiplication",col("OrderLineItem")*col("OrderQuantity")).display()

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@prjstorageone.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Data Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC - We are going to perform Aggregration
# MAGIC - requrement is to get the how many order were happen for every date 

# COMMAND ----------

# MAGIC %md
# MAGIC **To get the visulatzation just tab on + icon beside the table menu and from there you can choose visualtsation  from here we can also change the graph type and many more things **

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis 1

# COMMAND ----------

df_sales.groupBy("OrderDate").agg(count("OrderNumber").alias("TotalOrders")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis 2

# COMMAND ----------

df_prodCat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis 3

# COMMAND ----------

df_terr.display()

# COMMAND ----------

