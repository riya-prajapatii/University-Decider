# University-Decider

In this repository there is:

an SRC folder with:

the python code solving the problem: uni_climate_data.py
the csv of the data: final_project_table.csv
the db file of the data: university_climate.db
a csv of the summary table generated the pandas describe function: final_project_summary.csv
the final report
On the feature/API Call branch there is an API Code folder with:

edge case testing for the geolocation subroutine used in the final script: API_edge_case_testing.py
an early version of the API call code that used sample JSON data in lieu of making calls to the server: istorical_climate_data.py
an API_usage_stats.py script that the server uses to return usage data to the subscriber of the key
The code scraped a list of the top 100 universities in the US, and then makes an API request to www.weatherbit.io to retreive historical climate data for the colleges' unique locations. The data being queried here is based on average temperatures, precipitation, and snowfall for a given month based on data collected between 1991 and 2020. Once the data is retreived, this script then takes the average temperatures for each trimester of the college year (Fall = Sept-December, Winter = January-March, Spring = April-June). The script also returns the total precipitation and snowfall for the entire school year.

We used only a slice of the scraped list to limit the number of universities in our final table. The API has tight daily call limits for historical data that became apparent only after the small testing of the code was completed. Once run at full scale with 30 items in the table, the daily limit issue was raised. At most we could make sufficient calls to the API to cover 25 items, and we covered 24 to ensure that the code would run successfully. It appears that weatherbit.io prefers to sell its historical data through bulk orders of data sets rather than through free API calls. The API key here was also part of a free trial that expires on 06-12-2025
