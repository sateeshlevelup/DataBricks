# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ## SORTING

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Here are the sorting scenarios which we should be familiar with.
# MAGIC ## * Sort a data frame using ascending order by a specific column.
# MAGIC ## * Sort a data frame using descending order by a specific column.
# MAGIC ## * Dealing with nulls while sorting the data (having the null values at the beginning or at the end).
# MAGIC ## * Sort a data frame using multiple columns (composite sorting). We also need to be aware of how to sort the data in ascending order by first column and then descending order by second column as well as vice versa.
# MAGIC ## * We also need to make sure how to perform prioritized sorting. For example, let's say we want to get USA at the top and rest of the countries in ascending order by their respective names.

# COMMAND ----------

from pyspark.sql import Row
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
users_df = spark.createDataFrame(pd.DataFrame(users))
users_df.show()

# COMMAND ----------

#help(users_df.sort)
help(users_df.orderBy)

# COMMAND ----------


# Sort the **users** data in ascending order by **first_name**
from pyspark.sql.functions import col
#users_df.sort('first_name').show()
#users_df.sort(users_df.first_name).show()
#users_df.sort(users_df['first_name']).show()
#users_df.sort(col('first_name')).show()
users_df.sort('customer_from').show()


# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

users_df.sort(size('courses')).show()

# COMMAND ----------

users_df.sort('first_name', ascending=False).show()

# COMMAND ----------

users_df.select('id', 'courses').withColumn('no_of_courses', size('courses')).sort('no_of_courses', ascending=False).show()
   

# COMMAND ----------

users_df.sort(users_df['customer_from']).show()

# COMMAND ----------

users_df.sort(users_df['customer_from'].asc_nulls_last()).show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

users_df.sort(users_df['customer_from'].asc_nulls_last()).show()

# COMMAND ----------

users_df.sort(users_df['customer_from'].desc_nulls_last()).show()

# COMMAND ----------

courses = [{'course_id': 1,
3
  'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
4
  'suitable_for': 'Beginner',
5
  'enrollment': 1100093,
6
  'stars': 4.6,
7
  'number_of_ratings': 318066},
8
 {'course_id': 4,
9
  'course_name': 'Angular - The Complete Guide (2020 Edition)',
10
  'suitable_for': 'Intermediate',
11
  'enrollment': 422557,
12
  'stars': 4.6,
13
  'number_of_ratings': 129984},
14
 {'course_id': 12,
15
  'course_name': 'Automate the Boring Stuff with Python Programming',
16
  'suitable_for': 'Advanced',
17
  'enrollment': 692617,
18
  'stars': 4.6,
19
  'number_of_ratings': 70508},
20
 {'course_id': 10,
21
  'course_name': 'Complete C# Unity Game Developer 2D',
22
  'suitable_for': 'Advanced',
23
  'enrollment': 364934,
24
  'stars': 4.6,
25
  'number_of_ratings': 78989},
26
 {'course_id': 5,
27
  'course_name': 'Java Programming Masterclass for Software Developers',
28
  'suitable_for': 'Advanced',
29
  'enrollment': 502572,
30
  'stars': 4.6,
31
  'number_of_ratings': 123798},
32
 {'course_id': 15,
33
  'course_name': 'Learn Python Programming Masterclass',
34
  'suitable_for': 'Advanced',
35
  'enrollment': 240790,
36
  'stars': 4.5,
37
  'number_of_ratings': 58677},
38
 {'course_id': 3,
39
  'course_name': 'Machine Learning A-Z™: Hands-On Python & R In Data Science',
40
  'suitable_for': 'Intermediate',
41
  'enrollment': 692812,
42
  'stars': 4.5,
43
  'number_of_ratings': 132228},
44
 {'course_id': 14,
45
  'course_name': 'Modern React with Redux [2020 Update]',
46
  'suitable_for': 'Intermediate',
47
  'enrollment': 203214,
48
  'stars': 4.7,
49
  'number_of_ratings': 60835},
50
 {'course_id': 8,
51
  'course_name': 'Python for Data Science and Machine Learning Bootcamp',
52
  'suitable_for': 'Intermediate',
53
  'enrollment': 387789,
54
  'stars': 4.6,
55
  'number_of_ratings': 87403},
56
 {'course_id': 6,
57
  'course_name': 'React - The Complete Guide (incl Hooks, React Router, Redux)',
58
  'suitable_for': 'Intermediate',
59
  'enrollment': 304670,
60
  'stars': 4.6,
61
  'number_of_ratings': 90964},
62
 {'course_id': 18,
63
  'course_name': 'Selenium WebDriver with Java -Basics to Advanced+Frameworks',
64
  'suitable_for': 'Advanced',
65
  'enrollment': 148562,
66
  'stars': 4.6,
67
  'number_of_ratings': 49947},
68
 {'course_id': 21,
69
  'course_name': 'Spring & Hibernate for Beginners (includes Spring Boot)',
70
  'suitable_for': 'Advanced',
71
  'enrollment': 177053,
72
  'stars': 4.6,
73
  'number_of_ratings': 45329},
74
 {'course_id': 7,
75
  'course_name': 'The Complete 2020 Web Development Bootcamp',
76
  'suitable_for': 'Beginner',
77
  'enrollment': 270656,
78
  'stars': 4.7,
79
  'number_of_ratings': 88098},
80
 {'course_id': 9,
81
  'course_name': 'The Complete JavaScript Course 2020: Build Real Projects!',
82
  'suitable_for': 'Intermediate',
83
  'enrollment': 347979,
84
  'stars': 4.6,
85
  'number_of_ratings': 83521},
86
 {'course_id': 16,
87
  'course_name': 'The Complete Node.js Developer Course (3rd Edition)',
88
  'suitable_for': 'Advanced',
89
  'enrollment': 202922,
90
  'stars': 4.7,
91
  'number_of_ratings': 50885},
92
 {'course_id': 13,
93
  'course_name': 'The Complete Web Developer Course 2.0',
94
  'suitable_for': 'Intermediate',
95
  'enrollment': 273598,
96
  'stars': 4.5,
97
  'number_of_ratings': 63175},
98
 {'course_id': 11,
99
  'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
100
  'suitable_for': 'Beginner',
101
  'enrollment': 325047,
102
  'stars': 4.5,
103
  'number_of_ratings': 76907},
104
 {'course_id': 20,
105
  'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
106
  'suitable_for': 'Beginner',
107
  'enrollment': 203366,
108
  'stars': 4.6,
109
  'number_of_ratings': 45382},
110
 {'course_id': 2,
111
  'course_name': 'The Web Developer Bootcamp',
112
  'suitable_for': 'Beginner',
113
  'enrollment': 596726,
114
  'stars': 4.6,
115
  'number_of_ratings': 182997},
116
 {'course_id': 19,
117
  'course_name': 'Unreal Engine C++ Developer: Learn C++ and Make Video Games',
118
  'suitable_for': 'Advanced',
119
  'enrollment': 229005,
120
  'stars': 4.5,
121
  'number_of_ratings': 45860},
122
 {'course_id': 17,
123
  'course_name': 'iOS 13 & Swift 5 - The Complete iOS App Development Bootcamp',
124
  'suitable_for': 'Advanced',
125
  'enrollment': 179598,
126
  'stars': 4.8,
127
  'number_of_ratings': 49972}]
128
from pyspark.sql import Row
129
courses_df = spark.createDataFrame([Row(**course) for course in courses])
130
courses_df.dtypes
131
courses_df.show()
132
* Sort courses in ascending order by **suitable_for** and then in ascending order by **enrollment**.
133
courses_df. \
134
    sort('suitable_for', 'enrollment'). \
135
    show()
136
courses_df. \
137
    sort(courses_df['suitable_for'], courses_df['enrollment']). \
138
    show()
139
courses_df. \
140
    sort(['suitable_for', 'enrollment']). \
141
    show()
142
* Sort courses in ascending order by **suitable_for** and then in descending order by **number_of_ratings**.
143
courses_df. \
144
    sort(courses_df['suitable_for'], courses_df['number_of_ratings'].desc()). \
145
    show()
146
from pyspark.sql.functions import desc
147
courses_df. \
148
    sort(courses_df['suitable_for'], desc(courses_df['number_of_ratings'])). \
149
    show()
150
courses_df. \
151
    sort('suitable_for', desc('number_of_ratings')). \
152
    show()
153
courses_df. \
154
    sort(['suitable_for', 'number_of_ratings'], ascending=[1, 0]). \
155
    show()


# COMMAND ----------

courses = [{'course_id': 1,
  'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
  'suitable_for': 'Beginner',
  'enrollment': 1100093,
  'stars': 4.6,
  'number_of_ratings': 318066},
 {'course_id': 4,
  'course_name': 'Angular - The Complete Guide (2020 Edition)',
  'suitable_for': 'Intermediate',
  'enrollment': 422557,
  'stars': 4.6,
  'number_of_ratings': 129984},
 {'course_id': 12,
  'course_name': 'Automate the Boring Stuff with Python Programming',
  'suitable_for': 'Advanced',
  'enrollment': 692617,
  'stars': 4.6,
  'number_of_ratings': 70508},
 {'course_id': 10,
  'course_name': 'Complete C# Unity Game Developer 2D',
  'suitable_for': 'Advanced',
  'enrollment': 364934,
  'stars': 4.6,
  'number_of_ratings': 78989},
 {'course_id': 5,
  'course_name': 'Java Programming Masterclass for Software Developers',
  'suitable_for': 'Advanced',
  'enrollment': 502572,
  'stars': 4.6,
  'number_of_ratings': 123798},
 {'course_id': 15,
  'course_name': 'Learn Python Programming Masterclass',
  'suitable_for': 'Advanced',
  'enrollment': 240790,
  'stars': 4.5,
  'number_of_ratings': 58677},
 {'course_id': 3,
  'course_name': 'Machine Learning A-Z™: Hands-On Python & R In Data Science',
  'suitable_for': 'Intermediate',
  'enrollment': 692812,
  'stars': 4.5,
  'number_of_ratings': 132228},
 {'course_id': 14,
  'course_name': 'Modern React with Redux [2020 Update]',
  'suitable_for': 'Intermediate',
  'enrollment': 203214,
  'stars': 4.7,
  'number_of_ratings': 60835},
 {'course_id': 8,
  'course_name': 'Python for Data Science and Machine Learning Bootcamp',
  'suitable_for': 'Intermediate',
  'enrollment': 387789,
  'stars': 4.6,
  'number_of_ratings': 87403},
 {'course_id': 6,
  'course_name': 'React - The Complete Guide (incl Hooks, React Router, Redux)',
  'suitable_for': 'Intermediate',
  'enrollment': 304670,
  'stars': 4.6,
  'number_of_ratings': 90964},
 {'course_id': 18,
  'course_name': 'Selenium WebDriver with Java -Basics to Advanced+Frameworks',
  'suitable_for': 'Advanced',
  'enrollment': 148562,
  'stars': 4.6,
  'number_of_ratings': 49947},
 {'course_id': 21,
  'course_name': 'Spring & Hibernate for Beginners (includes Spring Boot)',
  'suitable_for': 'Advanced',
  'enrollment': 177053,
  'stars': 4.6,
  'number_of_ratings': 45329},
 {'course_id': 7,
  'course_name': 'The Complete 2020 Web Development Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 270656,
  'stars': 4.7,
  'number_of_ratings': 88098},
 {'course_id': 9,
  'course_name': 'The Complete JavaScript Course 2020: Build Real Projects!',
  'suitable_for': 'Intermediate',
  'enrollment': 347979,
  'stars': 4.6,
  'number_of_ratings': 83521},
 {'course_id': 16,
  'course_name': 'The Complete Node.js Developer Course (3rd Edition)',
  'suitable_for': 'Advanced',
  'enrollment': 202922,
  'stars': 4.7,
  'number_of_ratings': 50885},
 {'course_id': 13,
  'course_name': 'The Complete Web Developer Course 2.0',
  'suitable_for': 'Intermediate',
  'enrollment': 273598,
  'stars': 4.5,
  'number_of_ratings': 63175},
 {'course_id': 11,
  'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 325047,
  'stars': 4.5,
  'number_of_ratings': 76907},
 {'course_id': 20,
  'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
  'suitable_for': 'Beginner',
  'enrollment': 203366,
  'stars': 4.6,
  'number_of_ratings': 45382},
 {'course_id': 2,
  'course_name': 'The Web Developer Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 596726,
  'stars': 4.6,
  'number_of_ratings': 182997},
 {'course_id': 19,
  'course_name': 'Unreal Engine C++ Developer: Learn C++ and Make Video Games',
  'suitable_for': 'Advanced',
  'enrollment': 229005,
  'stars': 4.5,
  'number_of_ratings': 45860},
 {'course_id': 17,
  'course_name': 'iOS 13 & Swift 5 - The Complete iOS App Development Bootcamp',
  'suitable_for': 'Advanced',
  'enrollment': 179598,
  'stars': 4.8,
  'number_of_ratings': 49972}]

from pyspark.sql import Row
courses_df = spark.createDataFrame([Row(**course) for course in courses])
courses_df.dtypes
courses_df.show()

# COMMAND ----------

Sort Courses in ascending order by suitable_for and then in ascending order by enrollment

# COMMAND ----------

courses_df.sort(courses_df['suitable_for'],courses_df['enrollment']).show()

# COMMAND ----------

 Sort courses in ascending order by **suitable_for** and then in descending order by **number_of_ratings**

# COMMAND ----------

courses_df. \
    sort(courses_df['suitable_for'], courses_df['number_of_ratings'].desc()). \
    show()
