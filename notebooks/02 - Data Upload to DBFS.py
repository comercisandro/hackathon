# Databricks notebook source
# MAGIC %md
# MAGIC # Data Upload to DBFS
# MAGIC 
# MAGIC We can think in this notebook as the starting point. Data would come from different sources to this folder in dbfs.
# MAGIC 
# MAGIC We should be able to simulate as data is ingested monthly, to achieve that we should take data from reviews table for example, and split it by month.
# MAGIC 
# MAGIC Add noise to data. Null values, dates with wrong formats, podcasts with no rating, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC We can imagine this structure in DBFS
# MAGIC 
# MAGIC /dbfs/FileStore/SAME/
# MAGIC   - source1: Data coming from a transactional system. Tables are updated monthly.
# MAGIC     - reviews
# MAGIC     - categories
# MAGIC     - podcasts
# MAGIC     - etc.
# MAGIC   - source2: Data coming as CSV from other system that scraps data in a website. We'll need to fake this data or take a subset of previous DB.
# MAGIC     - reviews.csv with these fields
# MAGIC       - podcast_id:
# MAGIC       - category: Take existing categories from the other DB and we can add new ones.
# MAGIC       - review: If we want to fake this we can use OpenAI GPT-3 API.
# MAGIC       - rating: If we want to fake this we can use OpenAI GPT-3 API.
# MAGIC       - created_at: Fake this date field. It can be a totally different format than the DB data.

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/FileStore/SAME

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/FileStore/SAME/archive.zip -d /dbfs/FileStore/SAME/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/FileStore/SAME/

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def get_dataframe_from_sqlite_db(table_name):
# MAGIC     
# MAGIC     df = sqlContext.read.format('jdbc').\
# MAGIC          options(url='jdbc:sqlite:/dbfs/FileStore/SAME/database.sqlite',\
# MAGIC          dbtable=table_name,driver='org.sqlite.JDBC').load()
# MAGIC 
# MAGIC     return(df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # This cell will fetch data from every table in sqlite database, load it to a dataframe and the save it to Databricks FS, not to OS FS.
# MAGIC # If the files already exists it will prompt an error.
# MAGIC 
# MAGIC table_names = ["podcasts", "categories", "runs", "reviews"]
# MAGIC 
# MAGIC for table_name in table_names:
# MAGIC     
# MAGIC     df = get_dataframe_from_sqlite_db(table_name)
# MAGIC     
# MAGIC     df.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/dbfs/FileStore/SAME/{}".format(table_name))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Run this cell to remove files from Databricks File System (For common OS file removal you would use rm). 
# MAGIC # Folders should be empty to be removed.
# MAGIC 
# MAGIC for table_name in table_names:
# MAGIC     
# MAGIC     dbutils.fs.rm("/dbfs/FileStore/SAME/{}".format(table_name))

# COMMAND ----------

df = spark.sql("SELECT * FROM categories LIMIT 20")
display(df)

# COMMAND ----------

df = spark.sql("""SELECT 
                      b.category, 
                      COUNT(DISTINCT a.podcast_id) podcast_count, 
                      ROUND(AVG(rating),2) AS avg_rating,
                      PERCENTILE_APPROX(rating, 0.5) AS median_rating
                  FROM reviews a 
                  JOIN categories b
                  ON a.podcast_id=b.podcast_id
                  GROUP BY b.category""")
display(df)

# COMMAND ----------

df = spark.sql("""SELECT 
                      YEAR(created_at)*100 + MONTH(created_at) AS period,
                      COUNT(DISTINCT podcast_id) as podcast_count
                  FROM reviews
                  GROUP BY YEAR(created_at)*100 + MONTH(created_at)
                  ORDER BY YEAR(created_at)*100 + MONTH(created_at)""")
display(df)

# COMMAND ----------

def inject_wrong_format_dates(df, percentage, column_name):
    
    set.seed(1)
    df
    
2018-04-24T12:05:16-07:00
%Y-%m-%d

%m-%y-%d

df.replace()

# COMMAND ----------

# MAGIC %md ## Dashboard

# COMMAND ----------

podcast_df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("dbfs:/FileStore/SAME/podcasts.csv")

df.registerTempTable("podcast")

# COMMAND ----------

# MAGIC %sql SELECT podcast_id as podcast, itunes_id as itunes_id,  slug as slug
# MAGIC FROM podcast ORDER BY podcast_id

# COMMAND ----------

categories_df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("dbfs:/FileStore/SAME/categories.csv")

df.registerTempTable("categories")

# COMMAND ----------

# MAGIC %sql SELECT categories.title as Title, categories.itunes_id as itunes_ID
# MAGIC FROM categories ORDER BY itunes_ID

# COMMAND ----------


