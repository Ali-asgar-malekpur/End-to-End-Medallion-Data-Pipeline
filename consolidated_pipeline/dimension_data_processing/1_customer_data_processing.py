# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text('catalog', 'fmcg', 'Catalog')
dbutils.widgets.text('data_source', 'customers', 'Data Sources')

catalog = dbutils.widgets.get('catalog')
data_sources = dbutils.widgets.get('data_source')

print(catalog)


# COMMAND ----------

base_path = 'r2://sportsbar-dp@3be93c912191c1bf8cf7912818757bf4.r2.cloudflarestorage.com/customers'
print(base_path)

# COMMAND ----------

df = spark.read.format('csv')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(base_path)\
    .withColumn('read_timestamp', F.current_timestamp())\
    .select('*', '_metadata.file_name', '_metadata.file_size')

display(df.limit(10))


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
    .format('delta')\
    .option('delta.enableChangeDataFeed', 'true')\
    .mode('overwrite')\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_sources}")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_sources};")

df_bronze.show(10)

# COMMAND ----------

#Check for duplicate Customers
df_duplicates = df.groupBy('customer_id').count().where('count>1')
#df_duplicates = df.groupBy('customer_id').count().filter(F.col('count')>1)
display(df_duplicates)

# COMMAND ----------

#Dropping Duplicates
print(df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print(df_silver.count())

# COMMAND ----------

#Removing Trailing spaces
display(
    df_silver.filter(F.col('customer_name') != F.trim(F.col('customer_name')))
)

df_silver = df_silver.withColumn('customer_name', F.trim(F.col('customer_name')))

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)

df_silver.select('city').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
    'customer_name',
    F.when(F.col('customer_name').isNull(), None)
    .otherwise(F.initcap('customer_name'))
)

# COMMAND ----------

df_silver.filter(F.col('city').isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col('customer_name').isin(null_customer_names)).show()

# COMMAND ----------

customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
[(k,v) for k,v in customer_city_fix.items()],
['customer_id', 'fixed_city'])
display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, 'customer_id', 'left')
    .withColumn(
        'city',
        F.coalesce('city', 'fixed_city')
    ).drop('fixed_city')
)

# COMMAND ----------

# Sanity Checks

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: "CustomerName-City" or "CustomerName-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    
    # Static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_sources}")

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer!

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_sources};")


# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_sources}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

delta_table.alias('tager')
