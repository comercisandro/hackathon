# Databricks notebook source
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://database_server"
table = "schema.tablename"
user = ""
password = ""

# COMMAND ----------

remote_table = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", table)\
  .option("user", user)\
  .option("password", password)\
  .load()

# COMMAND ----------

display(remote_table.select("EXAMPLE_COLUMN"))

# COMMAND ----------

import sqlite3
import pandas as pd

database = "dbfs/FileStore/SAME/database.sqlite" 
conn = sqlite3.connect(database)

df = pd.read_sql("select * from sqlite_master", con=conn)
print(df.head())

conn.close()


#(spark.read.format('jdbc')
# .options(driver='org.sqlite.JDBC', dbtable='sqlite_master',url='jdbc:sqlite:sdxml.sqlite').load()
# .show(truncate=False)
#)
