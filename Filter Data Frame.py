# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Filtering DataFrame 
# MAGIC    #  Overview of Filter
# MAGIC    #  OverView of Condations and Operators for Filtering
# MAGIC    #  Filtering using Equal and NOT Equal
# MAGIC    #  Filtering Using Between Operator
# MAGIC    #  Dealing with null values 
# MAGIC    #  Filtering Data with  Boolen Vaules
# MAGIC    #  Filter with " <" ">"
# MAGIC    #  Working with "Boolean AND" and " Boolean OR" 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

import pandas as pd
from pyspark.sql import Row
import datetime
filter_df = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "gender": "male",
        "current_city": "Dallas",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "gender": "male",
        "current_city": "Houston",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "gender": "female",
        "current_city": "",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "gender": "male",
        "current_city": "San Fransisco",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "gender": "female",
        "current_city": None,
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)
filter_df1 = spark.createDataFrame(pd.DataFrame(users))
#filter_df1.show()
filter_df1.dtypes

# COMMAND ----------



# COMMAND ----------


#filter_df1.show()
display(filter_df1)
filter_df1.dtypes

# COMMAND ----------

help(filter_df1.filter)

# COMMAND ----------

help(filter_df1.filter)

# COMMAND ----------

filter_df1.filter(col('id')==1).show()

# COMMAND ----------

filter_df1.where(col('id')==1).show()

# COMMAND ----------

filter_df1.filter('id=1').show()

# COMMAND ----------

filter_df1.where('id=1').show()

# COMMAND ----------

filter_df1.createOrReplaceTempView('demo')

# COMMAND ----------

spark.sql("""
select * from demo
where id =1

""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Equal -> = or ==
# MAGIC # Not Equal -> !=
# MAGIC # Greater Than -> >
# MAGIC # Less Than -> <
# MAGIC # Greater Than or Equal To -> >=
# MAGIC # Less Than or Equal To -> <=
# MAGIC # IN Operator -> isin function or IN or contains function
# MAGIC # Between Operator -> between function or BETWEEN with AND

# COMMAND ----------

filter_df1.filter(col('is_customer')== True).show()

# COMMAND ----------

filter_df1.filter('is_customer= "true"').show()

# COMMAND ----------

filter_df1.where(col('is_customer')== True).show()

# COMMAND ----------

spark.sql('''
select * from demo
where is_customer ="true"

''').show()

# COMMAND ----------

filter_df1.filter(col('is_customer')== False).show()

# COMMAND ----------

filter_df1.filter('is_customer= "false"').show()

# COMMAND ----------

filter_df1.filter('current_city= "Dallas"').show()

# COMMAND ----------

filter_df1.filter(col('current_city')== "Dallas").show()

# COMMAND ----------

filter_df1.filter(col('amount_paid')== "900.0").show()

# COMMAND ----------

filter_df1.show()

# COMMAND ----------

from pyspark.sql.functions import isnan
from pyspark.sql.functions import isNull

# COMMAND ----------

filter_df1.select('amount_paid', isnan('amount_paid')).show()

# COMMAND ----------

filter_df1.filter(isnan('amount_paid')== True ).show()

# COMMAND ----------

display(filter_df1)

# COMMAND ----------

Get all the user who is not living in Dallas

# COMMAND ----------

filter_df1.filter('current_city != "Dallas"')|.show()

# COMMAND ----------

filter_df1.filter('current_city != "Dallas"').show()

# COMMAND ----------

filter_df1.filter((col('current_city')!= "Dallas")| col('current_city').isNull()).show()

# COMMAND ----------

> Get all the users whose city name is not empty string

# COMMAND ----------

filter_df1.filter((col('current_city') != '')| col('current_city').isNull()).show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

filter_df1.filter((col('current_city') != '')| col('current_city').isNull()).show()

# COMMAND ----------



# COMMAND ----------

filter_df1.filter((col('current_city')!= "Dallas")| col('current_city').isNull()).show()

# COMMAND ----------

Between operator
lower upper


# COMMAND ----------

Get user id and email whose last updated timestamp is btw 2021 feb15th and 2021 march 15th

# COMMAND ----------

display(filter_df1)

# COMMAND ----------

filter_df1.select('id','last_updated_ts').\
filter(col('last_updated_ts').between('2021-02-15 00:00:00','2021-03-15 15:16:52')).show()

# COMMAND ----------

filter_df1.select('id','last_updated_ts').show()

# COMMAND ----------

Get all the users payment between 850 and 900


# COMMAND ----------

filter_df1.select('id','amount_paid').show()

# COMMAND ----------


filter_df1.filter(col('amount_paid').between(850,900)).show()

# COMMAND ----------

filter_df1.select('id','amount_paid').filter('amount_paid BETWEEN 850 AND 900').show()

# COMMAND ----------

Handling Nulls
> is not null
> is null

Get all the users whose city is not null
Get all the users whose city is null
Get all the users where customer_from is null

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city')!=null).show()

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city')!=None).show()

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city').isNotNull()).show()

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city').isNull()).show()

# COMMAND ----------

filter_df1.select('id','customer_from').filter(col('customer_from').isNull()).show()

# COMMAND ----------

Get the list of users whose city is null or empty string
Get list of users whose city is either houston or dallas

# COMMAND ----------

filter_df1.select('id','current_city').show()

# COMMAND ----------

filter_df1.filter((col('current_city') == '')| (col('current_city').isNull())).show()

# COMMAND ----------

filter_df1.filter("current_city = '' OR current_city IS NULL").show()

# COMMAND ----------

filter_df1.filter((col('current_city') == 'Houston')| (col('current_city')== 'Dallas')).show()

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city').isin('Houston','Dallas')).show()

# COMMAND ----------

filter_df1.filter((col('amount_paid') >= 900)& (isnan(col('amount_paid')) == False)).show()

# COMMAND ----------

Boolen and condation
Get male customers ( gender = male)and is_customer = true

# COMMAND ----------

display(filter_df1)

# COMMAND ----------

filter_df1.select('id','gender','is_customer').show()

# COMMAND ----------

df2=filter_df1.filter((col('gender')== 'male') & (col('is_customer')== True))
df2.select('id','gender','is_customer').show()


# COMMAND ----------

Get id and email of users who are not customers or city contain empty string

# COMMAND ----------

df3 =filter_df1.filter((col('current_city')=='') | (col('is_customer')== False))

df3.select('id','current_city','is_customer').show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city') !=null).show()

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city') !=None).show()

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city').isNotNull()).show()

# COMMAND ----------

filter_df1.select('id','current_city').filter(col('current_city').isNull()).show()

# COMMAND ----------

filter_df1.select('id','last_updated_ts').\
filter(col('last_updated_ts').between('2021-02-15','2021-03-15')).show()
