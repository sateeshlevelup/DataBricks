# Databricks notebook source
storage_account_name = "demoinjection"
client_id            = "f7d59eb3-e21b-4bfc-812a-9915b2f553bf"
tenant_id            = "806d981e-b582-4a31-83ff-31c9bdc87fa6"
client_secret        = "pUi8Q~9wwL.AzXVF37QqbB5a2-~z63tULz1oQc6P"


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("sliver")

# COMMAND ----------

dbutils.fs.ls ("/mnt/demoinjection/sliver")


# COMMAND ----------

circuit=spark.read.option("header",True)\
.schema (cir_scheme)\
.csv("dbfs:/mnt/demoinjection/sliver/circuits.csv")

# COMMAND ----------

circuit.printSchema()

# COMMAND ----------

circuit.describe().show()


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

cir_scheme = StructType(fields=[StructField("circuitId",IntegerType(),False),
                               StructField("circuitRef",StringType(),True),
                               StructField("name",StringType(),True),
                               StructField("location",StringType(),True),
                               StructField("country",StringType(),True),
                               StructField("lat",DoubleType(),True),
                               StructField("lng",DoubleType(),True),
                               StructField("alt",StringType(),True),
                               StructField("url",StringType(),True) ])
                                
                                           
                                
                                           
                                                   

# COMMAND ----------

display(circuit)

# COMMAND ----------

ciruits2=circuit.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

ciruits21=circuit.select(circuit.circuitId,circuit.circuitRef,circuit.name,circuit.location,circuit.country,circuit.lat,circuit.lng,circuit.alt)

# COMMAND ----------

ciruits23=circuit.select(circuit["circuitId"],circuit["circuitRef"],circuit["name"],circuit["location"],circuit["country"],circuit["lat"],circuit["lng"],circuit["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

ciruits24=circuit.select(col("circuitId"),col("circuitRef"),col("name").alias("firstname"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(ciruits24)

# COMMAND ----------

ciruits24_renamed_df=ciruits24.withColumnRenamed("circuitId","circuit_Id" )\
.withColumnRenamed("circuitRef","circuit_ref" )\
.withColumnRenamed("lat","latitude" )\
.withColumnRenamed("lng","longitude" )\
.withColumnRenamed("alt","altitude" )


# COMMAND ----------

display(ciruits24_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuit_final=ciruits24_renamed_df.withColumn("ingestion_date",current_timestamp()) \
.withColumn ("env",lit("dev"))

# COMMAND ----------

mount_adls("gold")

# COMMAND ----------

circuit_final.write.mode("overwrite").parquet("/mnt/demoinjection/gold/circuits")

# COMMAND ----------

dfnew=spark.read.parquet("/mnt/demoinjection/gold/circuits")

# COMMAND ----------

display(dfnew)

# COMMAND ----------

# MAGIC %fs ls /mnt/demoinjection/gold/circuits

# COMMAND ----------

display(circuit_final)

# COMMAND ----------

display(ciruits23)

# COMMAND ----------

display(ciruits21)

# COMMAND ----------

display(ciruits2)
