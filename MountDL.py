# Databricks notebook source
storage_account_name = "datalakedemoacc"
client_id            = "49150740-f63c-464d-b6bb-9fb295e8c60e"
tenant_id            = "806d981e-b582-4a31-83ff-31c9bdc87fa6"
client_secret        = "m4w8Q~cKrM7m~yMHY69SgJUOZm.afACGZhYQkakJ"

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

mount_adls("raw")

# COMMAND ----------

dbutils.fs.ls ("/mnt/datalakedemoacc/raw")

# COMMAND ----------

df=spark.read.csv('/mnt/datalakedemoacc/raw/employees.csv',header=True,inferSchema=True )
display(df)

# COMMAND ----------

df2=df.select("EMPLOYEE_ID","FIRST_NAME","SALARY")
df2.show()

# COMMAND ----------

df2.write.mode("overwrite").csv("/mnt/datalakedemoacc/sliver/")
