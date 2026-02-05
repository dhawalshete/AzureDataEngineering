# Databricks notebook source
# MAGIC %md
# MAGIC # **SILVER LAYER SCRIPT**

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Data

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls abfss://bronze@projectonedatalake.dfs.core.windows.net

# COMMAND ----------

df_calendar = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Calendar/')

# COMMAND ----------

df_customers = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Customers/')

# COMMAND ----------

df_product_categories = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories/')

# COMMAND ----------

df_product_subcategories = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Product_Subcategories/')

# COMMAND ----------

df_products = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Products/')

# COMMAND ----------

df_returns = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Returns/')

# COMMAND ----------

df_sales = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_territories = spark.read.format('csv').options(Header=True, inferSchema=True).load('abfss://bronze@projectonedatalake.dfs.core.windows.net/AdventureWorks_Territories/')

# COMMAND ----------

df_calendar_transformed = (
                            df_calendar.withColumn('Year', year(col('Date')))
                                       .withColumn('Month', month(col('Date')))
                                       .withColumn('Day', day(col('Date')))
                        )

# df_calendar_transformed.display()

# COMMAND ----------

df_calendar_transformed.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Calendar').save()

# COMMAND ----------

df_customers_transformed = df_customers.withColumn('FullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

# COMMAND ----------

df_customers_transformed.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Customers').save()

# COMMAND ----------

df_product_subcategories.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Product_Subcategories').save()

# COMMAND ----------

df_products_transformed = (
                            df_products.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])
                                       .withColumn('ProductName', split(col('ProductName'), ' ')[0])
                        )
# df_products_transformed.display()

# COMMAND ----------

df_products_transformed.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Products').save()

# COMMAND ----------

# df_returns.display()
df_returns.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Returns').save()

# COMMAND ----------

# df_territories.display()
df_territories.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Territories').save()

# COMMAND ----------

# df_product_categories.display()
df_product_categories.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories').save()

# COMMAND ----------

df_sales_transformed = (
                        df_sales.withColumn('StockDate', to_timestamp(col('StockDate')))
                                .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))
                                .withColumn('Multipy', col('OrderLineItem') * col('OrderQuantity'))
                    )

df_sales_aggregated = df_sales_transformed.groupBy(col('OrderDate')).agg(count(col('OrderNumber')).alias('count_of_orders'))

# COMMAND ----------

# df_sales_aggregated.display()
df_sales_aggregated.write.format('parquet').mode('append').option('path', 'abfss://silver@projectonedatalake.dfs.core.windows.net/AdventureWorks_Sales').save()