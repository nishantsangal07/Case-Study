#!/usr/bin/env python
# coding: utf-8

# In[274]:


# !pip install findspark


# In[275]:


# import findspark
# findspark.init()
# findspark.find()


# In[276]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# In[277]:


# spark = SparkSession.builder.config("spark.driver.host","localhost").appName('case_study_pyspark').getOrCreate()


# In[278]:


#1. the flights data:

# flights=spark.read.format("flights.csv") .option("header", "true").option("inferschema", "true") 
flights = spark.read.csv("flights.csv",header=True,inferSchema=True)
flights.show()
flights.printSchema()


# In[279]:


# i) to check variables with sum of null values
from pyspark.sql.functions import *

flights.select([count(when(col(c).isNull(), c)).alias(c) for c in flights.columns]).show()


# In[280]:


# delete obs where 'dep_time' 'arr_time' 'tailnum' have null values

flights = flights.na.drop(subset=['dep_time', 'arr_time','tailnum'])


# In[281]:


flights.select([count(when(col(c).isNull(), c)).alias(c) for c in flights.columns]).show()


# In[282]:


# replacing null value with average  value of 'air_time' using spark sql and aggregate functions

# flights.createOrReplaceTempView("fly_data")
# avg_air=spark.sql("select carrier,origin,dest,avg(air_time) from fly_data group by carrier,origin,dest").show(truncate=False)  

flights_avg_air=flights.groupby('carrier', 'origin','dest').agg(mean('air_time').alias('mean_air_time'))

flights_avg_air.show()

flights = flights.join(flights_avg_air, on = ['origin','dest','carrier'],how = 'left')

flights = flights.withColumn('air_time', coalesce('air_time','mean_air_time'))

# dropping extra columns 

flights= flights.drop(col('mean_air_time'))


# In[283]:


flights.select([count(when(col(c).isNull(), c)).alias(c) for c in flights.columns]).show()


# In[284]:


# ii) weather data:
# Calculate the missing values present in each variable

weather = spark.read.csv("weather.csv",header=True,inferSchema=True)
weather.show()
weather.printSchema()


# In[285]:


# to check variables with sum of null values
from pyspark.sql.functions import *

weather.select([count(when(col(c).isNull(), c)).alias(c) for c in weather.columns]).show()


# In[286]:


#calculating average of variables having null values


weather_avg=weather.groupby('origin','date').agg(mean("temp"),mean("dewp"),mean("humid"),mean("wind_dir"),\
                                                 mean("wind_speed"),mean("wind_gust"),mean("pressure"))


# weather_avg.show()

weather = weather.join(weather_avg, on = ['origin','date'],how = 'left')

weather = weather.withColumn('temp' , coalesce('temp','avg(temp)'))\
                .withColumn('dewp' , coalesce('dewp','avg(dewp)'))\
                .withColumn('humid' , coalesce('humid','avg(humid)'))\
                .withColumn('wind_dir' , coalesce('wind_dir','avg(wind_dir)'))\
                .withColumn('wind_speed' , coalesce('wind_speed','avg(wind_speed)'))\
                .withColumn('wind_gust' , coalesce('wind_gust','avg(wind_gust)'))\
                .withColumn('pressure' , coalesce('pressure','avg(pressure)'))

# weather.show()

# dropping extra columns 

for i in weather.columns:
    if i[0:3]=='avg':
        weather = weather.drop(col(i))
        
# weather.show()


# In[287]:


# iii) the planes data:

planes = spark.read.csv("planes.csv",header=True,inferSchema=True)
planes.show()
planes.printSchema()


# In[288]:


# Calculating the missing values present in each variable

planes.select([count(when(col(c).isNull(), c)).alias(c) for c in planes.columns]).show()


# In[289]:


# Removing redundant variables with more than 70% missing values.

# per=planes.select([(count(when(col(c).isNull(), c))*100/count(lit(1))).alias(c) for c in planes.columns])
# per.show()

for i in planes.columns:
    if planes.select([(count(when(col(i).isNull(), i))*100/count(lit(1))).alias(i)]).collect()[0][0]>70:
        planes = planes.drop(col(i))
    
    
planes.select([count(when(col(c).isNull(), c)).alias(c) for c in planes.columns]).show()


# In[290]:


# Removing all the observations with any missing values

planes = planes.dropna(how = 'any')  
        
planes.select([count(when(col(c).isNull(), c)).alias(c) for c in planes.columns]).show()


# In[291]:


#2.Extracting information from the existing variables

flights = flights.withColumn('date',to_date(col('date'),"M/d/yyyy"))

flights.printSchema()

#Creating the new variables:

# test = flights.select(year(flights.date).alias('year'), month(flights.date).alias('month'), \
#                       dayofmonth(flights.date).alias('day'),hour(flights.date).alias('hour'))

flights = flights.withColumn('year', year(col('date')))
flights = flights.withColumn('month', month(col('date')))
flights = flights.withColumn('day',date_format('date','EEEE'))

flights.show(5)



# In[292]:


#fomatting time from int, float to datetime and calculating hour 

from  datetime import timedelta
from pyspark.sql.types import StringType
from pyspark.sql import functions as f

from pyspark.sql.functions import *

# flights = flights.withColumn("sched_dep_time",flights["sched_dep_time"].cast(StringType()))
# flights= flights.withColumn('sched_dep_time',f.concat(f.format_string("000"),f.col('sched_dep_time')))

flights = flights.withColumn('sched_dep_time',to_timestamp(col('sched_dep_time')))
flights = flights.withColumn('hour',hour(col('sched_dep_time')))


# In[293]:


flights = flights.withColumn('arr_delay',col('arr_time').cast(LongType())-col('sched_arr_time').cast(LongType()))
flights = flights.withColumn('dep_delay',col('dep_time').cast(LongType())-col('sched_dep_time').cast(LongType()))

flights.show(5)


# In[294]:


#3 Busiest Routes

busy_routes=flights.filter(flights.year == 2013).groupby('origin','dest').agg(count('date').alias("route_freq"))

#sort in descending order to find top busiest route 

busy_routes=busy_routes.sort(desc('route_freq'))
busy_routes.show(1)


# In[295]:


# number of flights for each of the carriers for the top five busiest routes

# busy_routes.printSchema()
# busy_routes=busy_routes.withColumn("row",row_number().over(route_freq)).filter(col("row") <= 5)

busy_routes=busy_routes.show(5)
top_five_busy_routes = busy_routes.join(flights, on = ['origin','dest'],how = 'left')

flights_busy_routes=flights.groupby("origin","dest","carrier").agg(count('flight').alias("count"))
df1=flights_busy_routes.show(16)


# In[296]:


# Compare the numbers calculated in (ii) with total number of flights for each carrier

df2=flights.groupby("carrier").agg(count('flight').alias("count"))
df2.show(16)


# In[297]:


df1.merge(df2,how="inner",on=["carrier"])


# In[298]:


# 4. Busiest time of the day (maximum flights taking off)

# carrier wise 

busy_hr_carr = flights.groupby("carrier","dep_time").agg(count('flight').alias("count"))

busy_hr_carr = busy_hr_carr.sort(desc('carrier'),desc('count'))

busy_hr_carr.show()


# In[299]:


# Busiest time of the day origin wise

busy_hr_org = flights.groupby("origin","dep_time").agg(count('flight').alias("count"))

busy_hr_org.groupby(["origin"]).max().show()


# In[300]:


# 5.  Origins and destinations 
# 
# out of total flights from JFK, percentage of flights got delayed

jfk=flights.filter((flights['dep_delay']>0) & (flights['origin']=='JFK')).count()/flights.filter\
(flights['origin']=='JFK').count()



# In[301]:


#No of flights from each origin airport which got delayed 

delays_flights_dest = flights.groupby('origin').agg(count('flight'))

delays_flights_dest.show(1)


# In[302]:


#  destination(s) has the highest delays

delays_flights_dest = flights.filter((flights['arr_delay']>0)).groupby('dest').agg(count('date')).sort(desc('count(date)'))


# In[303]:


# 7.    Years of operation and Fuel consumption cost

import matplotlib.pyplot as plt
import seaborn as sns

planes_agg = planes.groupby(['manufacturing_year']).agg(mean("fuel_cc"))

x = planes_agg.toPandas()['manufacturing_year'].values.tolist()
y = planes_agg.toPandas()['avg(fuel_cc)'].values.tolist()
plt.scatter(x,y)
plt.title("Manufacturing date vs Consumption Cost")
plt.xlabel("Manufacturing date")
plt.ylabel("Consumption Cost")

plt.show()


#  No, older planes do not use more fuel


# In[304]:


# understand check the relationships between fuel consumption with other plane variables 
# like number of seats, engine type, number of engines, type of plane


planes_type=planes.groupby(["type"]).agg(mean("fuel_cc"))
planes_type.show()

x = planes_type.toPandas()['type'].values.tolist()
y = planes_type.toPandas()['avg(fuel_cc)'].values.tolist()
plt.scatter(x,y)
plt.title("type vs fuel_cc")
plt.xlabel("type")
plt.ylabel("fuel_cc")

planes_engines=planes.groupby(["engines"]).agg(mean("fuel_cc"))

a = planes_engines.toPandas()['engines'].values.tolist()
b = planes_engines.toPandas()['avg(fuel_cc)'].values.tolist()
plt.scatter(a,b)
plt.title("engines vs fuel_cc")
plt.xlabel("engines")
plt.ylabel("fuel_cc")

planes_seats=planes.groupby(["seats"]).agg(mean("fuel_cc"))

a = planes_seats.toPandas()['seats'].values.tolist()
b = planes_seats.toPandas()['avg(fuel_cc)'].values.tolist()
plt.scatter(a,b)
plt.title("seats vs fuel_cc")
plt.xlabel("seats")
plt.ylabel("fuel_cc")


planes_engine=planes.groupby(["engine"]).agg(mean("fuel_cc"))

a = planes_engine.toPandas()['engine'].values.tolist()
b = planes_engine.toPandas()['avg(fuel_cc)'].values.tolist()
plt.scatter(a,b)
plt.title("engine vs fuel_cc")
plt.xlabel("engine")
plt.ylabel("fuel_cc")


# In[305]:


#8 Variation of delays over the course of the day

# delay=delays_data.loc[(delays_data.dep_delay>0) &(delays_data.arr_delay>0)]

# variation=delay.groupby(["hour"])["dep_delay"].mean().reset_index(name="Avg_Delay")
# variation.sort_values(by="hour")

# variation.plot(x="hour", y="Avg_Delay", kind="bar")



