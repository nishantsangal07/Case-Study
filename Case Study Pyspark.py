#!/usr/bin/env python
# coding: utf-8

# In[185]:


# !pip install findspark


# In[186]:


# import findspark
# findspark.init()
# findspark.find()


# In[187]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# In[188]:


# spark = SparkSession.builder.config("spark.driver.host","localhost").appName('case_study_pyspark').getOrCreate()


# In[189]:


#1. the flights data:

# flights=spark.read.format("flights.csv") .option("header", "true").option("inferschema", "true") 
# flights = spark.read.csv("flights.csv",header=True,inferSchema=True)
flights = spark.read.option("header", "true").csv("flights.csv",inferSchema=True)

flights.show(5)
flights.printSchema()

# test=flights.select('date','flight')
# test.show()


# In[190]:


# i) to check variables with sum of null values
from pyspark.sql.functions import *

flights.select([count(when(col(c).isNull(), c)).alias(c) for c in flights.columns]).show()


# In[191]:


# delete obs where 'dep_time' 'arr_time' 'tailnum' have null values

flights = flights.na.drop(subset=['dep_time', 'arr_time','tailnum'])


# In[192]:


flights.select([count(when(col(c).isNull(), c)).alias(c) for c in flights.columns]).show()


# In[193]:


# replacing null value with average  value of 'air_time' using spark sql and aggregate functions

#############  calculating mean using spark.sql       ########################3

# flights.createOrReplaceTempView("fly_data")
# avg_air=spark.sql("select carrier,origin,dest,avg(air_time) from fly_data group by carrier,origin,dest").show(truncate=False)  

flights_avg_air=flights.groupby('carrier', 'origin','dest').agg(mean('air_time').alias('mean_air_time'))

flights = flights.join(flights_avg_air, on = ['origin','dest','carrier'],how = 'left')

flights = flights.withColumn('air_time', coalesce('air_time','mean_air_time'))

# dropping extra columns 

flights= flights.drop(col('mean_air_time'))

#############  replacing mean using imputer    ########################3

# from pyspark.ml.feature import Imputer
# grouped_data = flights.groupBy(['carrier', 'origin','dest'])
# imputer=Imputer(inputCols=['air_time'], outputCols=['imputed_air_time']).setStrategy("mean")
# imputer.fit(grouped_data).transform(grouped_data).show()


# In[25]:


flights.select([count(when(col(c).isNull(), c)).alias(c) for c in flights.columns]).show()


# In[26]:


# ii) weather data:

weather = spark.read.csv("weather.csv",header=True,inferSchema=True)
weather.show(5)
weather.printSchema()


# In[36]:


# to check variables with sum of null values
from pyspark.sql.functions import *

#####checking values at null places #############3

weather.where("origin=='EWR' and humid='64.93'").select('origin','pressure','humid','wind_dir').distinct().collect()

######calculating sum of  null values 

weather.select([count(when(col(c)=='NA', c)).alias(c) for c in weather.columns]).show()


# In[37]:


### calculating average of variables having null values


weather_avg=weather.groupby('origin','date').agg(mean("temp"),mean("dewp"),mean("humid"),mean("wind_dir"),\
                                                 mean("wind_speed"),mean("wind_gust"),mean("pressure"))

weather = weather.join(weather_avg, on = ['origin','date'],how = 'left')

weather = weather.withColumn('temp' , coalesce('temp','avg(temp)'))\
                .withColumn('dewp' , coalesce('dewp','avg(dewp)'))\
                .withColumn('humid' , coalesce('humid','avg(humid)'))\
                .withColumn('wind_dir' , coalesce('wind_dir','avg(wind_dir)'))\
                .withColumn('wind_speed' , coalesce('wind_speed','avg(wind_speed)'))\
                .withColumn('wind_gust' , coalesce('wind_gust','avg(wind_gust)'))\
                .withColumn('pressure' , coalesce('pressure','avg(pressure)'))


######### dropping extra columns 

for i in weather.columns:
    if i[0:3]=='avg':
        weather = weather.drop(col(i))
 


# In[38]:


# iii) the planes data:

planes = spark.read.csv("planes.csv",header=True,inferSchema=True)
planes.show(5)
planes.printSchema()


# In[39]:


# Calculating the missing values present in each variable

planes.select([count(when(col(c).isNull(), c)).alias(c) for c in planes.columns]).show()


# In[40]:


# Removing redundant variables with more than 70% missing values.

# per=planes.select([(count(when(col(c).isNull(), c))*100/count(lit(1))).alias(c) for c in planes.columns])
# per.show()

for i in planes.columns:
    if planes.select([(count(when(col(i).isNull(), i))*100/count(lit(1))).alias(i)]).collect()[0][0]>70:
        planes = planes.drop(col(i))
    
    
planes.select([count(when(col(c).isNull(), c)).alias(c) for c in planes.columns]).show()


# In[41]:


# Removing all the observations with any missing values

planes = planes.dropna(how = 'any')  
       
### checking sum of null values

planes.select([count(when(col(c).isNull(), c)).alias(c) for c in planes.columns]).show()


# In[194]:


#2.Extracting information from the existing variables

###  converting date column datatype from string to date  ###############

flights = flights.withColumn('date',to_date(col('date'),"M/d/yyyy"))

flights.printSchema()

#### Creating the new variables YEAR, MONTH, DAY:

# test = flights.select(year(flights.date).alias('year'), month(flights.date).alias('month'), \
#                       dayofmonth(flights.date).alias('day'),hour(flights.date).alias('hour'))

flights = flights.withColumn('year', year(col('date')))
flights = flights.withColumn('month', month(col('date')))
flights = flights.withColumn('day',date_format('date','EEEE'))

flights.show(5)



# In[195]:


### fomatting time from int to datetime to calculate hour 

from  datetime import timedelta
from pyspark.sql.types import *
from pyspark.sql import functions as f

from pyspark.sql.functions import *
 
# test=flights.select('date','sched_arr_time','sched_dep_time','arr_time','dep_time')

# #######  converting int column to string in order to add string 000 #########

# test = test.withColumn("sched_dep_time",test.sched_dep_time.cast(StringType()))
# test= test.withColumn('sched_dep_time',f.concat(f.format_string("000"),f.col('sched_dep_time')))

# test= test.withColumn('f_sched_dep_tm',substring(test['sched_dep_time'],-7,5))
# test= test.withColumn('l_sched_dep_tm',substring(test['sched_dep_time'],-2,2))

# ######### calulating seconds available in x hours ##############

# test= test.withColumn('f_sched_dep_tm',test.f_sched_dep_tm.cast(IntegerType())*3600)\
#           .withColumn('l_sched_dep_tm',test.l_sched_dep_tm.cast(IntegerType())*60)
# test= test.withColumn('sched_dep_tm',test.f_sched_dep_tm+test.l_sched_dep_tm)

# ######### converting calculated seconds into timestamp  ############

# test= test.withColumn('sched_dep_tm',to_timestamp(col('sched_dep_tm')))
# test= test.withColumn('sched_dep_tm',test.sched_dep_tm - expr('INTERVAL 330 MINUTES'))

# ######### converting into string to substring date part from timestamp  ############

# test = test.withColumn("sched_dep_tm",test.sched_dep_tm.cast(StringType()))
# test= test.withColumn('sched_dep_time',substring(test['sched_dep_tm'],12,9))


# ######### dropping extra columns ###############

# for i in test.columns:
#     if i[0:2]=='f_' or i[0:2]=='l_' or i[-2:]=='tm':
#         test = test.drop(col(i))

######---------------------for sched_dep_time----------------------------------------------------------#################


#######  converting int column to string in order to add string 000 #########

flights = flights.withColumn("sched_dep_time",flights.sched_dep_time.cast(StringType()))
flights= flights.withColumn('sched_dep_time',f.concat(f.format_string("000"),f.col('sched_dep_time')))

flights= flights.withColumn('f_sched_dep_tm',substring(flights['sched_dep_time'],-7,5))
flights= flights.withColumn('l_sched_dep_tm',substring(flights['sched_dep_time'],-2,2))

######### calulating seconds available in x hours ##############

flights= flights.withColumn('f_sched_dep_tm',flights.f_sched_dep_tm.cast(IntegerType())*3600)\
          .withColumn('l_sched_dep_tm',flights.l_sched_dep_tm.cast(IntegerType())*60)
flights= flights.withColumn('sched_dep_tm',flights.f_sched_dep_tm+flights.l_sched_dep_tm)

######### converting calculated seconds into timestamp  ############

flights= flights.withColumn('sched_dep_tm',to_timestamp(col('sched_dep_tm')))
flights= flights.withColumn('sched_dep_tm',flights.sched_dep_tm - expr('INTERVAL 330 MINUTES'))

######### converting into string to substring date part from timestamp  ############

flights = flights.withColumn("sched_dep_tm",flights.sched_dep_tm.cast(StringType()))
flights= flights.withColumn('sched_dep_time',substring(flights['sched_dep_tm'],12,9))

######---------------------for dep_time----------------------------------------------------------#################


flights = flights.withColumn("dep_time",flights.dep_time.cast(StringType()))
flights= flights.withColumn('dep_time',f.concat(f.format_string("000"),f.col('dep_time')))

flights= flights.withColumn('f_dep_tm',substring(flights['dep_time'],-7,5))
flights= flights.withColumn('l_dep_tm',substring(flights['dep_time'],-2,2))

######### calulating seconds available in x hours ##############

flights= flights.withColumn('f_dep_tm',flights.f_dep_tm.cast(IntegerType())*3600)\
          .withColumn('l_dep_tm',flights.l_dep_tm.cast(IntegerType())*60)
flights= flights.withColumn('dep_tm',flights.f_dep_tm+flights.l_dep_tm)

######### converting calculated seconds into timestamp  ############

flights= flights.withColumn('dep_tm',to_timestamp(col('dep_tm')))
flights= flights.withColumn('dep_tm',flights.dep_tm - expr('INTERVAL 331 MINUTES'))

######### converting into string to substring date part from timestamp  ############

flights = flights.withColumn("dep_tm",flights.dep_tm.cast(StringType()))
flights= flights.withColumn('dep_time',substring(flights['dep_tm'],12,9))


# In[201]:


######---------------------for sched_arr_time----------------------------------------------------------#################


#######  converting int column to string in order to add string 000 #########

flights = flights.withColumn("sched_arr_time",flights.sched_arr_time.cast(StringType()))
flights= flights.withColumn('sched_arr_time',f.concat(f.format_string("000"),f.col('sched_arr_time')))

flights= flights.withColumn('f_sched_arr_tm',substring(flights['sched_arr_time'],-7,5))
flights= flights.withColumn('l_sched_arr_tm',substring(flights['sched_arr_time'],-2,2))

######### calulating seconds available in x hours ##############

flights= flights.withColumn('f_sched_arr_tm',flights.f_sched_arr_tm.cast(IntegerType())*3600)\
          .withColumn('l_sched_arr_tm',flights.l_sched_arr_tm.cast(IntegerType())*60)
flights= flights.withColumn('sched_arr_tm',flights.f_sched_arr_tm+flights.l_sched_arr_tm)

######### converting calculated seconds into timestamp  ############

flights= flights.withColumn('sched_arr_tm',to_timestamp(col('sched_arr_tm')))
flights= flights.withColumn('sched_arr_tm',flights.sched_arr_tm - expr('INTERVAL 330 MINUTES'))

######### converting into string to substring date part from timestamp  ############

flights = flights.withColumn("sched_arr_tm",flights.sched_arr_tm.cast(StringType()))
flights= flights.withColumn('sched_arr_time',substring(flights['sched_arr_tm'],12,9))


######---------------------for arr_time----------------------------------------------------------#################


flights = flights.withColumn("arr_time",flights.arr_time.cast(StringType()))
flights= flights.withColumn('arr_time',f.concat(f.format_string("000"),f.col('arr_time')))

flights= flights.withColumn('f_arr_tm',substring(flights['arr_time'],-7,5))
flights= flights.withColumn('l_arr_tm',substring(flights['arr_time'],-2,2))

######### calulating seconds available in x hours ##############

flights= flights.withColumn('f_arr_tm',flights.f_arr_tm.cast(IntegerType())*3600)\
          .withColumn('l_arr_tm',flights.l_arr_tm.cast(IntegerType())*60)
flights= flights.withColumn('arr_tm',flights.f_arr_tm+flights.l_arr_tm)

######### converting calculated seconds into timestamp  ############

flights= flights.withColumn('arr_tm',to_timestamp(col('arr_tm')))
flights= flights.withColumn('arr_tm',flights.arr_tm - expr('INTERVAL 331 MINUTES'))

######### converting into string to substring date part from timestamp  ############

flights = flights.withColumn("arr_tm",flights.arr_tm.cast(StringType()))
flights= flights.withColumn('arr_time',substring(flights['arr_tm'],12,9))


######### dropping extra columns ###############

for i in flights.columns:
    if i[0:2]=='f_' or i[0:2]=='l_' or i[-2:]=='tm':
        flights = flights.drop(col(i))

flights.printSchema()
flights.show(5)



# In[214]:


###converted date to string to merge in arr,dep columns #############

flights = flights.withColumn("date",flights.date.cast(StringType()))

######## merging date and arr,dep fields  ############

flights = flights.withColumn('sched_dep_time',concat(col('date'),format_string(" "),col('sched_dep_time')))
flights = flights.withColumn('sched_dep_time',to_timestamp('sched_dep_time'))

flights = flights.withColumn('dep_time',concat(col('date'),format_string(" "),col('dep_time')))
flights = flights.withColumn('dep_time',to_timestamp('dep_time'))

flights = flights.withColumn('sched_arr_time',concat(col('date'),format_string(" "),col('sched_arr_time')))
flights = flights.withColumn('sched_arr_time',to_timestamp('sched_arr_time'))

flights = flights.withColumn('arr_time',concat(col('date'),format_string(" "),col('arr_time')))
flights = flights.withColumn('arr_time',to_timestamp('arr_time'))

flights.printSchema()
flights.show(5)


# In[226]:


#### Creating the new variables HOUR,arr_delay, dep_delay in minutes:

flights = flights.withColumn('hour', hour(col('sched_dep_time')))

flights = flights.withColumn('dep_delay',col('dep_time').cast(LongType())-col('sched_dep_time').cast(LongType()))
flights=flights.withColumn('dep_delay',round(col('dep_delay')/60))

flights = flights.withColumn('arr_delay',col('arr_time').cast(LongType())-col('sched_arr_time').cast(LongType()))
flights=flights.withColumn('arr_delay',round(col('arr_delay')/60))

flights.show(5)


# In[ ]:





# In[303]:


#3 i) Busiest Routes
routes=flights.select('carrier','flight','origin','dest','date')

busy_routes=routes.filter(flights.year == 2013).groupby('origin','dest').agg(count('flight').alias("route_freq")).sort(desc('route_freq'))

##### Busiest Route ##############
busy_routes.show(1)


# In[311]:


# #3 ii) number of flights for each of the carriers for the top five busiest routes

# busy_routes.printSchema()

busy_routes.createOrReplaceTempView("routes")

busy_routes=spark.sql("select * from "+ " (select *, row_number() OVER (ORDER BY route_freq desc) as rn " +" FROM routes) where rn <=5 ")
busy_routes=busy_routes.drop('rn')
busy_routes.show()


top_five_busy_routes = busy_routes.join(routes, on = ['origin','dest'],how = 'left')

df1=top_five_busy_routes.groupby("origin","dest","carrier").agg(count('flight').alias("count")).sort("origin","dest","carrier")

df1.show()


# In[314]:


# 3 iii) Compare the numbers calculated in (ii) with total number of flights for each carrier

df2=routes.groupby("carrier").agg(count('flight').alias("count2")).sort("carrier")

df2.show()

df1.join(df2,how="inner",on=["carrier"]).sort("origin","dest","carrier").show()


# In[ ]:





# In[317]:


# 4. i) Busiest time of the day (maximum flights taking off)

# carrier wise 

busy_hr_carr = flights.groupby("carrier","hour").agg(count('flight').alias("count"))

busy_hr_carr.groupby(["carrier"]).max().sort("carrier").show()



# In[321]:


# 4 ii) Busiest time of the day origin wise

busy_hr_org = flights.groupby("origin","hour").agg(count('flight').alias("count"))

busy_hr_org.groupby(["origin"]).max().sort(desc('max(count)')).show()


# In[338]:


# 5. i) Origins and destinations 
# out of total flights from JFK, percentage of flights got delayed

delays_data=flights.select("origin","dest","flight","dep_delay","arr_delay","hour")

jfk=delays_data.where("origin=='JFK'").count()
jfk_delay=delays_data.where("dep_delay>0 and origin=='JFK'").count()

del_per=(jfk_delay)*100/jfk
del_per


# In[342]:


# 5 ii) origin airport had the least number of total delays

delays_flights=delays_data.where("dep_delay>0")

delays_flights_org = delays_flights.groupby('origin').agg(count('flight').alias("count"))

delays_flights_dest.sort('count').show(1)


# In[346]:


#  5 iii) destination(s) has the highest delays

delays_flights_dest = delays_flights.groupby('dest').agg(count('flight').alias("count"))

delays_flights_dest.sort(desc('count')).show()


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



