from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, countDistinct, min, max, mean, udf
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from datetime import datetime
from pyspark.sql.functions import *

# Initialise Spark session !!!!!!!!!!!!!!WE NEED TO KNOW HOW TO RUN IT IN DISTRIBUTED WAY
sc = SparkContext("local", "BikeShare")
spark = SparkSession.builder.appName('BikeShare').getOrCreate()


# Read in dataset "Santander_London_Stations.csv"
stationsLondonDF = spark.read.format("csv").options(delimiter = ';')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('/user/sgmpinol/data/Stations/Santander_London_Stations.csv')
# Change column names							 
newcolnames = ['DockingStationID','DockingStationName','StreetName','VillageLocality','Latitude','Longitude','NumberDockingPoints','InstallationDate','RemovalDate','CurrentStatus']
stationsLondonDF = stationsLondonDF.toDF(*newcolnames)

# Read in dataset "Capital_Washington_Stations.csv"
terminalsWashingtonDF = spark.read.format("csv").options(delimiter = ';')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('/user/sgmpinol/data/Stations/Capital_Washington_Stations.csv')
# Change column names							 
newcolnames = ['Address','TerminalNumber','Latitude','Longitude','NumberDockingPoints']
terminalsWashingtonDF = terminalsWashingtonDF.toDF(*newcolnames)

# Read in dataset "dates.csv"
datesDF = spark.read.format("csv").options(delimiter = ';')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('/user/sgmpinol/data/Calendar/dates.csv')


# Read in dataset "festivity_london.csv"
holidaysLondonDF = spark.read.format("csv").options(delimiter = ';')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('/user/sgmpinol/data/Calendar/festivity_london.csv')

# Read in dataset "holidays_washington.csv"
holidaysWashingtonDF = spark.read.format("csv").options(delimiter = ';')\
                             .options(header=True)\
                             .options(inferSchema=True)\
                             .load('/user/sgmpinol/data/Calendar/holidays_washington.csv')

########## INITIAL EXPLORATION ##########

## STATIONS LONDON ##

print('\n\n --------- LONDON STATIONS ---------\n')	

print('\n\n Dataset: \n')	
stationsLondonDF.show(n = 5)

#replace "NULL" with Null
#print('\n\n replace "NULL" strings with Null (None)  value\n')
stationsLondonDF =  stationsLondonDF.withColumn("VillageLocality",
    when(
        col("VillageLocality").isin('NULL'),
        None
    ).otherwise(col("VillageLocality"))
)

# if station has been removed then is not available
#print('\n\n if removal date != null then Installed:Anavailable \n')
stationsLondonDF = stationsLondonDF.withColumn("CurrentStatus", \
              when(stationsLondonDF["RemovalDate"] != "-", "Installed:Anavailable").otherwise(stationsLondonDF["CurrentStatus"]))


stationsLondonDF.cache()
print('stationsLondonDF Count rows:',stationsLondonDF.count())

print('\n\n*) Dataframe Schema:\n')
stationsLondonDF.printSchema()

# Null values x column
print('\n\n*) Null values x column\n')	
stationsLondonDF.select([count(when(col(c).isNull(), c)).alias(c) for c in stationsLondonDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
print('\n\nSummary statistics of dataframe:\n')
stationsLondonDF.describe().show()

## we need to transform InstallationDate and RemovalDate from string to date
stationsLondonDF = stationsLondonDF.withColumn('InstallationDate', from_unixtime(unix_timestamp('InstallationDate', 'dd/mm/yyyy')).alias('InstallationDate'))
stationsLondonDF = stationsLondonDF.withColumn('RemovalDate', from_unixtime(unix_timestamp('RemovalDate', 'dd/mm/yyyy')).alias('RemovalDate'))
stationsLondonDF = stationsLondonDF.withColumn("InstallationDate", stationsLondonDF["InstallationDate"].cast(DateType())).withColumn("RemovalDate", stationsLondonDF["RemovalDate"].cast(DateType()))

print('\n\nAfter adjusting schema:\n')
print('\n\n*)Dataframe New Schema:\n')
stationsLondonDF.printSchema()

# Null values x column
print('\n\n*) Null values x column\n')	
stationsLondonDF.select([count(when(col(c).isNull(), c)).alias(c) for c in stationsLondonDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
print('\n\nSummary statistics of dataframe:\n')
stationsLondonDF.describe().show()

stationsLondonDF.select(count('InstallationDate'),min('InstallationDate'),max('InstallationDate')).show()
stationsLondonDF.select(count('RemovalDate'),min('RemovalDate'),max('RemovalDate')).show()

stationsLondonDF.groupBy('CurrentStatus').count().show()

## TERMINALS WASHINGTON ##	

print('\n\n --------- WASHINGTON TERMINALS ---------\n')	

print('\n\n Dataset: \n')							 
terminalsWashingtonDF.show(n = 5)

terminalsWashingtonDF.cache()
print('terminalsWashingtonDF Count rows:',terminalsWashingtonDF.count())

print('\n\n*) Dataframe Schema:\n')
terminalsWashingtonDF.printSchema()

# Null values x column
print('\n\n*) Null values x column\n')	
terminalsWashingtonDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in terminalsWashingtonDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')
print('\n\nSummary statistics of dataframe:\n')	
terminalsWashingtonDF.describe().show()
terminalsWashingtonDF.select(countDistinct("Address")).show()
terminalsWashingtonDF.select(countDistinct("TerminalNumber"),min("TerminalNumber"),max("TerminalNumber")).show()

## DATES ##	

print('\n\n --------- DATES ---------\n')	

print('\n\n Dataset: \n')	
datesDF.show(n = 5)

datesDF.cache()
print('datesDF Count rows:',datesDF.count())

print('\n\n*) Dataframe Schema:\n')
datesDF.printSchema()

# Null values x column
print('\n\n*) Null values x column\n')	
datesDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in datesDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
datesDF.select(count("Date"),min("Date"),max("Date")).show()
datesDF.groupBy('DayOfWeek').count().show()

## we need to transform Date from string to date
datesDF = datesDF.withColumn('Date', from_unixtime(unix_timestamp('Date', 'dd/mm/yyyy')).alias('Date'))
datesDF = datesDF.withColumn("Date", datesDF["Date"].cast(DateType()))
## drop empty rows
datesDF = datesDF.dropna()

print('\n\nAfter adjusting schema and deleting empty rows:\n')
print('\n\n*)Dataframe New Schema:\n')
datesDF.printSchema()

# Null values x column
print('\n\n*) Null values x column\n')	
datesDF.select([count(when(col(c).isNull(), c)).alias(c) for c in datesDF.columns]).show()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
datesDF.select(count("Date"),min("Date"),max("Date")).show()
datesDF.groupBy('DayOfWeek').count().show()


## HOLIDAYS LONDON ##	

print('\n\n --------- HOLIDAYS LONDON ---------\n')	

print('\n\n Dataset: \n')	
holidaysLondonDF.show(n = 5)

holidaysLondonDF.cache()
print('holidaysLondonDF Count rows:',holidaysLondonDF.count())

print('\n\n*) Dataframe Schema:\n')
holidaysLondonDF.printSchema()

# Null values x column
print('\n\n*) Null values x column\n')	
holidaysLondonDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in holidaysLondonDF.columns]).show()

## we need to transform Date from string to date
holidaysLondonDF = holidaysLondonDF.withColumn('Date', from_unixtime(unix_timestamp('Date', 'dd/mm/yyyy')).alias('Date'))
holidaysLondonDF = holidaysLondonDF.withColumn("Date", holidaysLondonDF["Date"].cast(DateType()))

print('\n\nAfter adjusting schema:\n')
print('\n\n*)Dataframe New Schema:\n')
holidaysLondonDF.printSchema()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
holidaysLondonDF.select(count("Date"),min("Date"),max("Date")).show()
holidaysLondonDF.groupBy('Festivity').count().show()

## HOLIDAYS WASHINGTON ##	

print('\n\n --------- HOLIDAYS WASHINGTON ---------\n')	

print('\n\n Dataset: \n')	
holidaysWashingtonDF.show(n = 5)

holidaysWashingtonDF.cache()
print('holidaysWashingtonDF Count rows:',holidaysWashingtonDF.count())

print('\n\n*) Dataframe Schema:\n')
holidaysWashingtonDF.printSchema()

# Null values x column
print('\n\n*) Null values x column\n')	
holidaysWashingtonDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in holidaysWashingtonDF.columns]).show()

## we need to transform Date from string to date
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Date', from_unixtime(unix_timestamp('Date', 'dd/mm/yyyy')).alias('Date'))
holidaysWashingtonDF = holidaysWashingtonDF.withColumn("Date", holidaysWashingtonDF["Date"].cast(DateType()))

print('\n\nAfter adjusting schema:\n')
print('\n\n*)Dataframe New Schema:\n')
holidaysWashingtonDF.printSchema()

# Descriptive statistics
print('\n\n*) Descriptive statistics\n')	
holidaysWashingtonDF.select(count("Date"),min("Date"),max("Date")).show()
holidaysWashingtonDF.groupBy('Festivity').count().show(truncate=False)


#delete space after New Year's Day
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Festivity', regexp_replace('Festivity', "New Year's Day ", "New Year's Day"))

#Correct Martin Luther King Jr
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Festivity', regexp_replace('Festivity', "Martin Luther King Jr. Birthday", "Martin Luther King"))
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Festivity', regexp_replace('Festivity', "Martin Luther King Jr. Day", "Martin Luther King"))

#Correct Washington's Birthday
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Festivity', regexp_replace('Festivity', "Washington's Birthday ", "Birthday of Washington"))
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Festivity', regexp_replace('Festivity', "Washington's Birthday", "Birthday of Washington(Presidents' Day)"))

#Correct Indigenous - Colombus Day
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Festivity', regexp_replace('Festivity', "Indigenous Peoples' Day", "Columbus Day"))
holidaysWashingtonDF = holidaysWashingtonDF.withColumn('Festivity', regexp_replace('Festivity', "Columbus Day", "Columbus Day/ Indigenous Peoples' Day"))


#  N E W  Festivity / Count table
print('\n\n*) NEW Festivity - Count table\n')
holidaysWashingtonDF.groupBy('Festivity').count().show(truncate=False)
