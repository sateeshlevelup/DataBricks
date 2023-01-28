# Databricks notebook source
storage_account_name = "demoinjection"
client_id            = "f7d59eb3-e21b-4bfc-812a-9915b2f553bf"
tenant_id            = "806d981e-b582-4a31-83ff-31c9bdc87fa6"
client_secret        = "pUi8Q~9wwL.AzXVF37QqbB5a2-~z63tULz1oQc6P"




configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)



mount_adls("sliver")

# COMMAND ----------

dbutils.fs.ls ("/mnt/demoinjection/sliver")

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/demoinjection/sliver/constructors.json")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

schema= "constructorId INT,constructorRef STRING, name STRING,nationality STRING,url STRING"

# COMMAND ----------

df=spark.read\
.schema(schema) \
.json("dbfs:/mnt/demoinjection/sliver/constructors.json")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

drop_df=df.drop(col('url'))
display(drop_df)

# COMMAND ----------



# COMMAND ----------

df_final=drop_df.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("constructorRef","constructor_ref")\
.withColumn("ingestion_date",current_timestamp())

display(df_final)
          

# COMMAND ----------

df_final.write.mode("overwrite").parquet("dbfs:/mnt/demoinjection/gold/constructors")

# COMMAND ----------

driver=spark.read.json("dbfs:/mnt/demoinjection/sliver/drivers.json")
display(driver)

# COMMAND ----------

from pyspark.sql.types import  StructType,StructField,IntegerType,StringType, DateType

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

n_schema=StructType(fields=[StructField("forename",StringType(),True),
 StructField("surname",StringType(),True)])

# COMMAND ----------

d_schema = StructType(fields=[StructField("code",StringType(), False ),
                             StructField("dob",DateType(), True),
                             StructField("driverId",IntegerType(), True),
                             StructField("driverRef",StringType(), True),
                             StructField("name",n_schema),
                             StructField("nationality",StringType(),True),
                             StructField("number",IntegerType(),True),
                             StructField("url",StringType(),True) 
                              ])

# COMMAND ----------

driver_df=spark.read\
.schema(d_schema) \
.json("dbfs:/mnt/demoinjection/sliver/drivers.json")

# COMMAND ----------

display(driver_df)

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

driver_m =driver_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref" )\
.withColumn("ingestion_date",current_timestamp()) \
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))


# COMMAND ----------

display(driver_m)

# COMMAND ----------

driver_final=driver_m.drop(col("url"))
driver_final.write.mode("overwrite").parquet("dbfs:/mnt/demoinjection/gold/drivers")

# COMMAND ----------

pit_stop = StructType(fields=[StructField("raceId",IntegerType(), False ),
                             StructField("driverId",IntegerType(), True),
                             StructField("stop",StringType(), True),
                             StructField("lap",IntegerType(), True),
                             StructField("time",StringType()),
                             StructField("duration",StringType(),True),
                             StructField("milliseconds",IntegerType(),True)
                             
                              ])

# COMMAND ----------

pit_stops_df=spark.read \
.schema(pit_stop ) \
.option("multiline",True) \
.json("dbfs:/mnt/demoinjection/sliver/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

pit_stops_df.write.mode("overwrite").parquet("dbfs:/mnt/demoinjection/gold/pitshops")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Data Injection  Multiple files

# COMMAND ----------

laptimes = StructType(fields=[StructField("raceId",IntegerType(), False ),
                             StructField("driverId",IntegerType(), True),
                             StructField("stop",StringType(), True),
                             StructField("lap",IntegerType(), True),
                             StructField("position",IntegerType()),
                             StructField("time",StringType(),True),
                             StructField("milliseconds",IntegerType(),True)
                             
                              ])

# COMMAND ----------

lap_df=spark.read \
.schema(laptimes) \
.csv("dbfs:/mnt/demoinjection/sliver/lap_times")

# COMMAND ----------

display(lap_df)

# COMMAND ----------

lap_df.count()

# COMMAND ----------

lap_df.write.mode("overwrite").partitionBy('driverId').parquet("dbfs:/mnt/demoinjection/gold/lap")
