#!/usr/bin/env python
# coding: utf-8

# In[149]:


#1.i. the flights data:

import pandas as pd
flights=pd.read_csv("flights.csv")
# flights_copy=pd.read_csv("flights.csv")
flights.head()


# In[150]:


flights.isnull().sum()


# In[151]:


flights = flights.dropna(subset=['dep_time', 'arr_time','tailnum'])


# In[152]:


flights.isnull().sum()


# In[153]:


flights["air_time"].fillna(flights.groupby(['carrier', 'origin','dest'])['air_time'].transform('mean'),inplace=True)


# In[154]:


flights.isnull().sum()


# In[66]:


flights.loc[(flights.carrier=='US') & (flights.origin=='EWR')& (flights.dest=='CLT')&(flights.dep_time==1824) 
            &(flights.tailnum=='N176UW'),['date','sched_dep_time','dep_time','carrier','origin','dest','air_time']]


# In[67]:


#flights.loc[(flights.air_time.isnull()==True) , ['dep_time','carrier','origin','dest','air_time']]\


# In[135]:


#1.ii the weather data

import pandas as pd
weather=pd.read_csv("weather.csv")
weather.head(5)


# In[136]:


weather.isnull().sum()


# In[70]:


weather['temp'].fillna(weather.groupby(['origin','date'])['temp'].transform('mean'),inplace=True)


# In[71]:


weather.isnull().sum()
#weather.loc[(weather.origin=='EWR')& (weather.date=='8/22/2013'),['origin','date','temp','wind_speed']]


# In[146]:


# for i in weather:
#     print(i)
# weather.columns


# In[100]:


#1.iii the planes data

import pandas as pd 
import numpy as np

planes=pd.read_csv("planes.csv")
planes.info()
planes.head(5)


# In[101]:


# Calculating the missing values present in each variable
planes.isnull().sum()


# In[102]:


# Removing redundant variables with more than 70% missing values.
per=planes.isnull().sum()/(len(planes))*100
display(per)
planes=planes.drop(['speed'],axis=1)


# In[103]:


# Removing all the observations with any missing values

planes.dropna(how='any',inplace=True)
planes.isnull().sum()


# In[155]:


#2.Extracting information from the existing variables

flights.info()
flights.head(2)


# In[156]:


#Creating the new variables:


import datetime as dt
import numpy as np
flights['date']=pd.to_datetime(flights['date'])

flights['year']=flights['date'].dt.year
flights['month']=pd.DatetimeIndex(flights['date']).month
#flights['Date']=flights['date'].dt.day
flights['days']=flights['date'].dt.day_name()
#flights=flights.drop(['Year','Month','Date','Days'],axis=1)
flights.loc[(flights.carrier=='UA')&(flights.dest=='IAH')]


# In[157]:


#fomatting time from int, float to datetime 

from  datetime import timedelta

flights['sched_dep_time']= '000'+ flights['sched_dep_time'].astype(str)
flights['sched_dep_time']= flights['sched_dep_time'].apply(lambda t:  int(t[:-2])*3600+int(t[-2:])*60)
sch_dep_sec=flights['sched_dep_time']
flights['sched_dep_time']=flights['sched_dep_time'].apply(lambda sch_dep_sec: str(timedelta(seconds=sch_dep_sec)))


flights['dep_time']= flights['dep_time'].apply(lambda t: int(t))
flights['dep_time']= '000'+flights['dep_time'].astype(str)
flights['dep_time']= flights['dep_time'].apply(lambda t:  int(t[:-2])*3600+int(t[-2:])*60-1)
dep_sec=flights['dep_time']
flights['dep_time']=flights['dep_time'].apply(lambda dep_sec: str(timedelta(seconds=dep_sec)))


flights['sched_arr_time']= '000'+ flights['sched_arr_time'].astype(str)
flights['sched_arr_time']= flights['sched_arr_time'].apply(lambda t:  int(t[:-2])*3600+int(t[-2:])*60)
sch_arr_sec=flights['sched_arr_time']
flights['sched_arr_time']=flights['sched_arr_time'].apply(lambda sch_arr_sec: str(timedelta(seconds=sch_arr_sec)))


flights['arr_time']= flights['arr_time'].apply(lambda t: int(t))
flights['arr_time']= '000'+flights['arr_time'].astype(str)
flights['arr_time']= flights['arr_time'].apply(lambda t:  int(t[:-2])*3600+int(t[-2:])*60-1)
arr_sec=flights['arr_time']
flights['arr_time']=flights['arr_time'].apply(lambda arr_sec: str(timedelta(seconds=arr_sec)))

flights.head()


# In[158]:


#concat date and time to find departure and arrival delay

import numpy as np

import datetime as dt
flights['date']= flights['date'].astype(str)

flights['sched_dep_time']= flights['date'] +" "+flights['sched_dep_time']
flights['sched_dep_time']=flights['sched_dep_time'].astype('datetime64[ns]')

flights['dep_time']= flights['date'] +" "+flights['dep_time']
flights['dep_time']=flights['dep_time'].astype('datetime64[ns]')

flights['sched_arr_time']= flights['date'] +" "+flights['sched_arr_time']
flights['sched_arr_time']=flights['sched_arr_time'].astype('datetime64[ns]')

flights['arr_time']= flights['date'] +" "+flights['arr_time']
flights['arr_time']=flights['arr_time'].astype('datetime64[ns]')

flights['dep_delay']=(flights['dep_time']-flights['sched_dep_time'])/np.timedelta64(1,"m")
flights['arr_delay']=(flights['arr_time']-flights['sched_arr_time'])/np.timedelta64(1,"m")

flights['hour']=flights['sched_dep_time'].dt.hour

flights.head()


# In[159]:


#3 Busiest Routes

routes=flights[['carrier','flight','origin','dest']]
busy_routes=routes.groupby(['origin', 'dest']).size().reset_index(name='route_freq')
busy_routes.nlargest(1,["route_freq"])


# In[160]:


# number of flights for each of the carriers for the top five routes

busy_routes=busy_routes.nlargest(5,["route_freq"])
top_five_routes=busy_routes.merge(routes,how='left',on=['origin','dest'])
display(busy_routes)
a=top_five_routes.groupby(["origin","dest","carrier"]).size().reset_index(name="count")
df1=a[["origin","dest","carrier","count"]].head(16)
df1


# In[161]:


# Compare the numbers calculated in (ii) with total number of flights for each carrier
df2=routes.groupby(["carrier"])["flight"].size().reset_index(name="count2")
df2


# In[162]:


#df1.reset_index(drop=True)==df2.reset_index(drop=True)
df1.merge(df2,how="inner",on=["carrier"])


# In[163]:


#4. Busiest time of the day (maximum flights taking off)
# carrier wise

busy_hr_carr=flights.groupby(["carrier","hour"])["flight"].size().reset_index(name="count")
busy_hr_carr.groupby(["carrier"]).max()


# In[164]:


# Busiest time of the day origin wise

busy_hr_org=flights.groupby(["origin","hour"])["flight"].size().reset_index(name="count")
busy_hr_org.groupby(["origin"]).max()


# In[165]:


# 5. Origins and destinations 

# out of total flights from JFK, percentage of flights got delayed

delays_data=flights[["origin","dest","flight","dep_delay","arr_delay","hour"]]
jfk=len(delays_data.loc[(delays_data.origin=='JFK')])
jfk_delay=len(delays_data.loc[(delays_data.origin=='JFK') & (delays_data.dep_delay>0)])
del_per=(jfk_delay)*100/jfk
del_per
# 328063 109284 41129


# In[991]:


#origin airport had the least number of total delays

delays_flights=delays_data.loc[(delays_data.dep_delay>0)]
delays_flights_org=delays_flights.groupby(["origin"])["flight"].size().reset_index(name="count")
delays_flights_org.nsmallest(1,"count")


# In[992]:


#  destination(s) has the highest delays

delays_flights_dest=delays_flights.groupby(["dest"])["flight"].size().reset_index(name="count")
delays_flights_dest.nlargest(1,"count")


# In[171]:


# 6). Understanding weather conditions related with delays

import datetime as dt
weather['date']=pd.to_datetime(weather['date'])
weather['date']= weather['date'].astype(str)
weather['hour_sched_dep']=weather['hour_sched_dep'].astype('datetime64[ns]')
weather['hour']=weather['hour_sched_dep'].dt.hour
weather.head()


# In[172]:


flights.head()


# In[175]:


# 6.i) Join the weather and flights data using the variables: date, hour and origin variables.

weather_flight=flights[["month","date","origin","hour","dep_delay","arr_delay"]].merge(weather[["date","origin","hour","temp","wind_dir","wind_speed",
                                                           "pressure","visib"]],how='inner',on=["date","origin","hour"])
weather_flight.loc[(weather_flight.dep_delay>0)]

weather_flight.to_csv("weather_flight.csv",index=False)

weather_flight.head()


# In[180]:


# 6. ii) Calculate averages for the weather condition parameters provided and the departure delay, grouped by months.

avg_weather=weather_flight.groupby(["month"])["dep_delay","temp"].mean()

avg_weather


# In[92]:


planes.head()


# In[127]:


# 7.  Years of operation and Fuel consumption cost
import matplotlib.pyplot as plt

planes.groupby(["manufacturing_year"])["fuel_cc"].mean().reset_index(name="avg_fuel_cc").plot(kind="line")

#  No, older planes do not use more fuel

# plt.scatter(planes_data['manufacturing_year'], planes_data['avg_fuel_cc'])
# plt.show()


# In[114]:


pip install seaborn


# In[134]:


# understand check the relationships between fuel consumption with other plane variables 
# like number of seats, engine type, number of engines, type of plane

planes_data2=planes.groupby(["type","engines","seats","engine"])["fuel_cc"].mean().reset_index(name="avg_fuel_cc")
# planes_data2['parameters']=planes_data2["type"]+" "+planes_data2["engines"].map(str)+" "+planes_data2["seats"].map(str)+" "+planes_data2["engine"]
planes_data2.head()
# plane=planes_data2[["parameters","avg_fuel_cc"]]

# plane.plot(kind="line")
import seaborn as sns
sns.pairplot(planes_data2)


# In[1015]:


# understand check the relationships between fuel consumption with other plane variables 
# like number of seats, engine type, number of engines, type of plane

planes_data2=planes.groupby(["type","engines","seats","engine"])["fuel_cc"].mean().reset_index(name="avg_fuel_cc")
planes_data2['parameters']=planes_data2["type"]+" "+planes_data2["engines"].map(str)+" "+planes_data2["seats"].map(str)+" "+planes_data2["engine"]
# planes_data2.head()
import matplotlib.pyplot as plt
plt.figure().set_figwidth(25)
planes_data2.plot(x="parameters", y="avg_fuel_cc", kind="bar")


# In[1016]:


# 8. Variation of delays over the course of the day

delay=delays_data.loc[(delays_data.dep_delay>0) &(delays_data.arr_delay>0)]

variation=delay.groupby(["hour"])["dep_delay"].mean().reset_index(name="Avg_Delay")
variation.sort_values(by="hour")

variation.plot(x="hour", y="Avg_Delay", kind="bar")


# In[ ]:




