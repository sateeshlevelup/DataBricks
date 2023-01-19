# Databricks notebook source
# MAGIC %md
# MAGIC ## Dropping
# MAGIC ## Sorting

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
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
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]
import pandas as pd
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)
drop= spark.createDataFrame(pd.DataFrame(users))
#drop.show()
#display(drop)
drop.printSchema()

# COMMAND ----------

help(drop.drop)

# COMMAND ----------

#drop.drop(col('email')).show()
drop.drop(drop.email).show()

# COMMAND ----------

drop.drop(col('phone_numbers')).show()

# COMMAND ----------

drop.drop('first_name','amount').show()

# COMMAND ----------

lst_col =['first_name','last_name','email','phone_numbers']

# COMMAND ----------

lst_col1=drop.drop(*lst_col)

# COMMAND ----------

lst_col1.show()

# COMMAND ----------

drop.drop('email', 'phone_numbers').show()
drop.drop('email').show()


# COMMAND ----------



# COMMAND ----------

#drop.drop(col('email')).show()
#drop.drop(col('email')).printSchema()
drop.drop(drop.email).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop Duplicates based on all columns,it is also knows as distinct
# MAGIC # Drop duplicates based on certain values
# MAGIC # we can use distinct, drop_duplicates or dropDuplicates for these scenarios

# COMMAND ----------

users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
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
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
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
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
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
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 25, 3, 33, 0)
    }
]


import pandas as pd
sort_df = spark.createDataFrame(pd.DataFrame(users))
sort_df.show()
sort_df.count()


# COMMAND ----------

sort_df.distinct().show()

# COMMAND ----------

sort_df.dropDuplicates(['id']).show()

# COMMAND ----------

sort_df.dropDuplicates(['id','amount_paid']).show()

# COMMAND ----------

help(users_df.distinct)
# Drops exact duplicates
users_df.distinct().show()
users_df.distinct().count()
help(users_df.drop_duplicates)
help(users_df.dropDuplicates)
# We can also drop duplicates based on certain columns
# This will fail as the function expects sequence type object such as list or array
users_df.dropDuplicates('id').show()
users_df.dropDuplicates(['id']).show()
users_df.dropDuplicates(['id', 'amount_paid']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Here are the features that are available with respect to dropping records based on null values.
# MAGIC * Drop records when all column values are nulls.
# MAGIC * Drop records any of the column value is null.
# MAGIC * Drop records that have less than `thresh` non-null values.
# MAGIC * Drop records when any of the column value or all column values are nulls for provided subset of columns.
# MAGIC * We can use `df.na.drop` or `df.dropna` to take care of dealing with records having columns with null values.

# COMMAND ----------

08 Dropping Null based Records from Spark Data Frames
%md

Here are the features that are available with respect to dropping records based on null values.
* Drop records when all column values are nulls.
* Drop records any of the column value is null.
* Drop records that have less than `three` non-null values.
* Drop records when any of the column value or all column values are nulls for provided subset of columns.
* We can use `df.na.drop` or `df.dropna` to take care of dealing with records having columns with null values.
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
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
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
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
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
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
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 5,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 25, 3, 33, 0)
    }
]


import pandas as pd
users_df = spark.createDataFrame(pd.DataFrame(users))
users_df.show()
users_df.count()
# Attribute which exposes functions dealing with null records
# drop => users_df.dropna
# fill => users_df.fillna
# replace => users_df.replacena
users_df.na
help(users_df.dropna)
help(users_df.na.drop)
users_df.show()
users_df.na.drop('all').show()
# Drop if any column value is null
users_df.na.drop('any').show()
users_df.show()
users_df.na.drop(thresh=2).show()
users_df.show()
users_df.na.drop(how='all', subset=['id', 'email']).show()
users_df.na.drop(how='any', subset=['id', 'email']).show()

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
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
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
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
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
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
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 5,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 25, 3, 33, 0)
    }
]

import pandas as pd
drop1_df = spark.createDataFrame(pd.DataFrame(users))
drop1_df.show()
drop1_df.count()


# COMMAND ----------

help(drop1_df.dropna)

# COMMAND ----------

drop1_df.show()
drop1_df.na.drop('all').show()

# COMMAND ----------

drop1_df.na.drop('any').show()

# COMMAND ----------

drop1_df.na.drop(thresh=5).show()

# COMMAND ----------

#display(drop1_df)
drop1_df.na.drop(how='all', subset=['id', 'email']).show()

# COMMAND ----------

drop1_df.na.drop(how='any', subset=['id', 'email']).show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

drop1_df.na.drop(how='all', subset=['id', 'email']).show()
#drop1_df.na.drop(how='any', subset=['id', 'email']).show()
