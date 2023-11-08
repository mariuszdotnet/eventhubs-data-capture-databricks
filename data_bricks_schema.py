# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location and type
# MAGIC
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "###"
storage_account_access_key = "###"

# COMMAND ----------

# Recursive folder File Lookup
file_location = "wasbs://event-hub-d@ehcapturemk.blob.core.windows.net/mk-demo/event-hub-d/0/2023/11/"
file_type = "avro"

# COMMAND ----------

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC First, let's create a DataFrame in Python.
# MAGIC
# MAGIC EventHubs capture schema can be found here: https://learn.microsoft.com/en-us/azure/event-hubs/explore-captured-avro-files

# COMMAND ----------

from pyspark.sql.avro.functions import from_avro

# Recursive File Lookup
df = spark.read.format(file_type).option("inferSchema", "true").option("recursiveFileLookup", "true").load(file_location)

# You can also use a Schema Registry - https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/avro-dataframe
jsonFormatSchema = """
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}"""

df = df.withColumn("body",from_avro("body", jsonFormatSchema))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Create Temporary View from Json
# MAGIC
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

# Show JSON column
display(df.select("body"))

# Create new DF with JSON columns
df2 = df.select("body.*")

# Show columns
display(df2.select("*"))

# Create Temp View
df2.createOrReplaceTempView("TempView")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: Query the data 
# MAGIC
# MAGIC Convert the JSON Object to Table Format

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SELECT EXAMPLE_GROUP, SUM(EXAMPLE_AGG) FROM YOUR_TEMP_VIEW_NAME GROUP BY EXAMPLE_GROUP
# MAGIC SELECT * from TempView WHERE favorite_color="blue"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
