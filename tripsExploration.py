# READ IN DATASETS

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, countDistinct, min, max, mean, udf
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from datetime import datetime

# Initialise Spark session !!!!!!!!!!!!!!WE NEED TO KNOW HOW TO RUN IT IN DISTRIBUTED WAY
sc = SparkContext("local", "BikeShare")
spark = SparkSession.builder.appName('BikeShare').getOrCreate()


# Read in dataset TripsWashington folder in a dataframe
tripsWashingtonDF = spark.read.format("csv").options(delimiter = ',')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('data/TripsWashington/*.csv')
# Change column names							 
newcolnames = ['Duration','StartDate','EndDate','StartStationNumber','StartStation','EndStationNumber','EndStation','BikeNumber','Membertype']
tripsWashingtonDF = tripsWashingtonDF.toDF(*newcolnames)

# Read in dataset JourneysLondon folder in a dataframe
journeysLondonDF = spark.read.format("csv").options(delimiter = ',')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('data/JourneysLondon/*.csv')
# Change column names							 
newcolnames = ['RentalId','Duration','BikeId','EndDate','EndStationId','EndStationName','StartDate','StartStationId','StartStationName']
journeysLondonDF = journeysLondonDF.toDF(*newcolnames)

########## INITIAL EXPLORATION ##########
							 
## TRIPS WASHINGTON ##	

print('\n\nWASHINGTON TRIPS\n')						 
tripsWashingtonDF.show()
tripsWashingtonDF.describe().show()

# Null values x column
print('\n\n*) Null values x column\n')	
tripsWashingtonDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in tripsWashingtonDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
tripsWashingtonDF.select(countDistinct("StartStationNumber")).show()
tripsWashingtonDF.select(countDistinct("StartStation")).show()
tripsWashingtonDF.select(countDistinct("EndStationNumber")).show()
tripsWashingtonDF.select(countDistinct("EndStation")).show()
tripsWashingtonDF.groupBy('Membertype').count().show()


## JOURNEYS LONDON ##

print('\n\nLONDON JOURNEYS\n')	
journeysLondonDF.show()
journeysLondonDF.describe().show()

# Null values x column
print('\n\n*) Null values x column\n')	
journeysLondonDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in journeysLondonDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
journeysLondonDF.select(countDistinct("RentalId")).show()
journeysLondonDF.select(countDistinct("BikeId")).show()
journeysLondonDF.select(countDistinct("EndStationId")).show()
journeysLondonDF.select(countDistinct("EndStationName")).show()
journeysLondonDF.select(countDistinct("StartStationId")).show()
journeysLondonDF.select(countDistinct("StartStationName")).show()

# Close Spark session
sc.stop()
