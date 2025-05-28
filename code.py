# Code for scraping the top US colleges and their historical climate data.

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from geopy.geocoders import Nominatim
import time
import sqlite3

url_list = {
     'uni':['https://www.topuniversities.com/where-to-study/north-america/united-states/ranked-top-100-us-universities#page-1']
}
def get_university_data(url_list):
    universities = []
    countries = []
    for url in url_list:
        page = requests.get(url)

        if page.status_code == 200:
            soup = BeautifulSoup(page.content, 'html.parser')
            name_tags = soup.find_all('div', class_='uni_name')

            uni_list=[]
            for name_tag in name_tags:
              uni_list.append(name_tag.find_all('a'))

            for uni in uni_list:
              universities.append(uni[0].get_text())

            place_list=[]
            for name_tag in name_tags:
              place_list.append(name_tag.find_all('span'))
            for place in place_list:
              countries.append(place[0].get_text())

    return universities, countries

# Used only a slice of the scraped list to limit the number of universities in our 
# final table. The API has tight daily call limits for historical data that became
# apparent only after the small testing of the code was completed. Once run at full 
# scale with 30 items in the table, the daily limit issue was raised. At most we 
# could make sufficient calls to the API to cover 25 items, and we covered 24 to ensure 
# that the code would run successfully. It appears that weatherbit.io prefers to sell
# its historical data through bulk orders of data sets rather than through free API 
# calls. The API key here was also part of a free trial that expires on 06-12-2025

universities, cities = get_university_data(url_list['uni'])
cities = cities[0:20]
universities = universities[0:20]

data = {
    'University': universities,
    'Country': cities    
    }

df = pd.DataFrame(data)  # this is the data upon which the APi calls will be based

#display (df)

# *****************************************************
# **  End of first block of code that scrapes        **
# **  Start of the historical climate data API call  **
# *****************************************************

# The code below makes an API request to www.weatherbit.io to retreive historical
# climate data for unique locations. The data being queried here is based on average
# temperatures, precipitation, and snowfall for a given month based on data
# collected between 1991 and 2020.

# Once the data is retreived, this script then takes the average temperatures for
# each trimester of the college year (Fall = Sept-December, Winter = January-March,
# Spring = April-June). The script also returns the total precipitation and snowfall
# for the entire school year.


# getInfo is the function call that returns the historical climate data for each 
# location based on latitude and longitude.

def getInfo(lat,lon):
  url = f'https://api.weatherbit.io/v2.0/normals?lat={lat}&lon={lon}&start_day=01-30&end_day=12-01&units=I&tp=monthly&key=API_KEY'
  response = requests.get(url)
  if response.status_code == 200:
    data = response.json()
    return data
  else:
    print (f'the request failed (error {response.status_code})')

# Since the API will only return historical climate data based on the latitude
# and longitude of one of our cities in the list, latlong is the function call
# that translates our list of cities into a usable geolocation point.

def latlong(city):
  geolocator = Nominatim(user_agent="CIS9650")
  location = geolocator.geocode(city)
  lat = location.latitude
  lon = location.longitude
  loc = str(lat), str(lon)
  return (loc)

# get_climate_data is a loop of API calls that constructs a list of climate data 
# sets for every city. Rate limits in the API require this efficiency. 
# By doing this, for each season we no longer need to query the API again. 
# Since the underlying data is pulled for the year, it can yield season data 
# without additional API calls

def get_climate_data():
  all_climates = []
  for city in cities:
    location = latlong(f'{city},USA') # USA added to each city to ensure correct geolocation
    time.sleep (5)  # a little delay to avoid making too many requests per second
    weather = getInfo(location[0],location[1])  # the raw json data
    all_climates.append(weather)
  return all_climates

# get_season is the funtion that processes the raw historical climate data
# for the final table of data.

def get_season(season):
  temp_list=[]    #lists where month-on-month weather point data are assembled
  rain_list=[]
  snow_list=[]
  count = 0
  for city in cities:
    year = (all_climates[count]["data"])  # the relevant json data returned by the API
    count += 1
    total_temp = 0
    total_snow = 0
    total_rain = 0
    for month in year:
      if month['month'] in season:  # ensures function returns values for correct trimester months
        total_temp += (month['temp'])
        total_snow += (month['snow'])
        total_rain += (month['precip'])
      else: continue

    avg_temp = round(total_temp / len(season),1) # calculates average temp for trimester

    temp_list.append (avg_temp)
    snow_list.append (total_snow)  # collecting total rain and snow per trimester
    rain_list.append (total_rain)  # trimester data summed in the dataframe to reduce API calls
    
  dict = {      # dictionary of lists to build the dataframe for each trimester
    'City':cities,
    'Avg Temp':temp_list,
    'Total Rain':rain_list,
    'Total Snow':snow_list
      }
  return dict


#  * Working part of API-call code *

all_climates = get_climate_data()

tri_one = get_season([9,10,11,12])  # pulls climate data for sep (9) - dec (12)
df1 = pd.DataFrame(tri_one)

tri_two = get_season([1,2,3])
df2 = pd.DataFrame(tri_two)
df2=df2.drop(columns=['City'])      # merged data frames on index instead of city:
                                    # duplicate values for cities created combinatory
tri_three = get_season([4,5,6])     # multiplication of items during the data frame
df3 = pd.DataFrame(tri_three)       # merges below
df3=df3.drop(columns=['City'])


print('School Year Historical Climate Data by Trimester')
print('------------------------------------------------')

# dataframes for the three trimesters are combined here based on index.
merged_df = pd.merge(df1, df2, left_index=True, right_index=True)
all_merge = pd.merge(merged_df, df3, left_index=True, right_index=True)

# columns renamed to make display of dataframe meaningfull
all_merge = all_merge.rename(columns={
    'Avg Temp_x':'T1 Avg Temp','Total Rain_x':'T1 Rain','Total Snow_x':'T1 Snow',
    'Avg Temp_y':'T2 Avg Temp','Total Rain_y':'T2 Rain','Total Snow_y':'T2 Snow',
    'Avg Temp':'T3 Avg Temp','Total Rain':'T3 Rain','Total Snow':'T3 Snow'
    })

# rain and snow from the three dataframes are added together here:
all_merge['Total SY Snow'] = round(all_merge[['T1 Snow','T2 Snow','T3 Snow']].sum(axis=1),1)
all_merge['Total SY Rain'] = round(all_merge[['T1 Rain','T2 Rain','T3 Rain']].sum(axis=1),1)

# since total snow and rain data for the whole year is all that is required,
# the columns with the trimester totals for rain and snow are removed from final display.
final_climate_table = all_merge.drop(columns=['T1 Snow','T2 Snow','T3 Snow','T1 Rain','T2 Rain','T3 Rain'])

# final_project_table is the data frame of the scrape and the combined data frames of the
# climate tables, and now ready to be saved as a csv and database.
# duplicate columnm for country (actually a city) is removed
final_project_table = pd.merge(df,final_climate_table, left_index=True, right_index=True)
final_project_table = final_project_table.drop(columns=['Country'])

display (final_project_table)

#final df descriptive statistics

print(final_project_table.describe())

#export final df to csv file

final_project_table.to_csv("final_project_table.csv", index=False)


#final_project_table to sql

conn = sqlite3.connect("university_climate.db")
cursor = conn.cursor()
final_project_table.to_sql("universitydata", conn, if_exists="replace", index=False)
