-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS same_bronze_db

-- COMMAND ----------

df = SELECT * same_bronze_db.reviews

def prep_data()


df_clean = 



CREATE DATABASE IF NOT EXISTS same_silver_db

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS same_gold_db

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS same_bronze_db.reviews (podcast_id STRING, title STRING, content STRING, rating INT, author_id STRING, created_at TIMESTAMP, year_month INT)
    PARTITIONED BY (year_month);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS same_bronze_db.podcasts (podcast_id STRING, itunes_id BIGINT, slug STRING, itunes_url STRING, title STRING);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS same_bronze_db.categories (podcast_id STRING, category STRING);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS same_silver_db.podcasts_reviews (podcast_id STRING, category STRING, title STRING, review_title STRING, review_content STRING, rating INT, created_at TIMESTAMP);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS same_gold_db.categories_stats (category STRING, podcast_count INT, median_rating FLOAT, avg_rating FLOAT);

-- COMMAND ----------

-- CREATE TABLE IF NOT EXISTS same_gold_db.rating_stats (rating int, podcast_count INT, median_rating FLOAT, avg_rating FLOAT);
