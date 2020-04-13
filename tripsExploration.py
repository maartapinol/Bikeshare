from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id


# Initialise Spark session !!!!!!!!!!!!!!WE NEED TO KNOW HOW TO RUN IT IN DISTRIBUTED WAY
sc = SparkContext("local", "BikeShare")
spark = SparkSession.builder.appName('BikeShare').getOrCreate()


# Read in dataset TripsWashington folder in a dataframe
tripsWashingtonDF = spark.read.format("csv").options(delimiter = ',')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('/user/sgmpinol/data/TripsWashington/*.csv')
# Change column names							 
newcolnames = ['Duration','StartDate','EndDate','StartStationNumber','StartStation','EndStationNumber','EndStation','BikeNumber','Membertype']
tripsWashingtonDF = tripsWashingtonDF.toDF(*newcolnames)

# Read in dataset JourneysLondon folder in a dataframe
journeysLondonDF = spark.read.format("csv").options(delimiter = ',')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('/user/sgmpinol/data/JourneysLondon/*.csv')
# Change column names							 
newcolnames = ['RentalId','Duration','BikeId','EndDate','EndStationId','EndStationName','StartDate','StartStationId','StartStationName']
journeysLondonDF = journeysLondonDF.toDF(*newcolnames)

########## INITIAL EXPLORATION ##########
							 
## TRIPS WASHINGTON ##	

print('\n\n --------- WASHINGTON TRIPS --------- \n')						 
tripsWashingtonDF.show(n = 5)
tripsWashingtonDF.describe().show()

tripsWashingtonDF.cache()
print('tripsWashingtonDF Count rows:', tripsWashingtonDF.count())

print('\n\n*) Dataframe Schema:\n')
tripsWashingtonDF.printSchema()

# add column with identification number
tripsWashingtonDF = tripsWashingtonDF.withColumn("TripID", monotonically_increasing_id())

# N E W WASHINGTON
print('\n\n N E W  WASHINGTON TRIPS\n')
tripsWashingtonDF.show()
tripsWashingtonDF.describe().show()


# Most Popular stations and how many times has been found
print('\n\n*) Most Popular Stations in Washington:\n')
tripsWashingtonDF.groupBy('StartStation').count().orderBy('count', ascending=False).show(truncate=False)
tripsWashingtonDF.groupBy('EndStation').count().orderBy('count', ascending=False).show(truncate=False)


# Null values x column
print('\n\n*) Null values x column\n')	
tripsWashingtonDF.select([count(when(col(c).isNull(), c)).alias(c) for c in tripsWashingtonDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')
tripsWashingtonDF.select(min('StartDate'),max('StartDate')).show()
tripsWashingtonDF.select(min('EndDate'),max('EndDate')).show()	
tripsWashingtonDF.select(countDistinct("StartStationNumber")).show()
tripsWashingtonDF.select(countDistinct("StartStation")).show()
tripsWashingtonDF.select(countDistinct("EndStationNumber")).show()
tripsWashingtonDF.select(countDistinct("EndStation")).show()
tripsWashingtonDF.groupBy('Membertype').count().show()


## JOURNEYS LONDON ##

print('\n\nLONDON JOURNEYS\n')	
journeysLondonDF.show(n = 5)
journeysLondonDF.describe().show()

journeysLondonDF.cache()
print('journeysLondonDF Count rows:', journeysLondonDF.count())

print('\n\n*) Dataframe Schema:\n')
journeysLondonDF.printSchema()

## we need to transform StartDate and EndDate from string to timestamp
journeysLondonDF = journeysLondonDF.withColumn('StartDate', from_unixtime(unix_timestamp('StartDate', 'dd/mm/yyyy HH:mm:ss')).alias('StartDate'))
journeysLondonDF = journeysLondonDF.withColumn('EndDate', from_unixtime(unix_timestamp('EndDate', 'dd/mm/yyyy HH:mm:ss')).alias('EndDate'))
journeysLondonDF = journeysLondonDF.withColumn("StartDate", journeysLondonDF["StartDate"].cast(DateType())).withColumn("EndDate", journeysLondonDF["EndDate"].cast(DateType()))

print('\n\nAfter adjusting schema:\n')
print('\n\n*)Dataframe New Schema:\n')
journeysLondonDF.printSchema()

# Most Popular stations and how many times has been found
print('\n\n*) Most Popular Stations in London:\n')      
journeysLondonDF.groupBy('StartStationName').count().orderBy('count', ascending=False).show(truncate=False)
journeysLondonDF.groupBy('EndStationName').count().orderBy('count', ascending=False).show(truncate=False)


# Null values x column
print('\n\n*) Null values x column\n')	
journeysLondonDF.select([count(when(col(c).isNull(), c)).alias(c) for c in journeysLondonDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')
tripsWashingtonDF.select(min('StartDate'),max('StartDate')).show()
tripsWashingtonDF.select(min('EndDate'),max('EndDate')).show()		
journeysLondonDF.select(countDistinct("BikeId")).show()
journeysLondonDF.select(countDistinct("EndStationId")).show()
journeysLondonDF.select(countDistinct("EndStationName")).show()
journeysLondonDF.select(countDistinct("StartStationId")).show()
journeysLondonDF.select(countDistinct("StartStationName")).show()

# Close Spark session
sc.stop()
