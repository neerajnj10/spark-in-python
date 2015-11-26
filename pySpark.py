#Loading in a DataFrame

- To create a Spark DataFrame we load an external DataFrame, called mtcars. This DataFrame includes 32 observations on 11 variables.

[, 1] mpg Miles/(US) --> gallon
[, 2] cyl --> Number of cylinders
[, 3] disp --> Displacement (cu.in.)
[, 4] hp --> Gross horsepower
[, 5] drat --> Rear axle ratio
[, 6] wt --> Weight (lb/1000)
[, 7] qsec --> 1/4 mile time
[, 8] vs --> V/S
[, 9] am --> Transmission (0 = automatic, 1 = manual)
[,10] gear --> Number of forward gears
[,11] carb --> Number of carburetors
In [ ]:

import pandas as pd
import importr 
mtcars = pd.read_csv('mtcars.csv')
mtcars.head()

#Initialize SQLContext

To work with dataframes we need a SQLContext which is created using SQLContext(sc). SQLContext uses SparkContext(sp) created.

sqlContext = SQLContext(sc)

#Creating Spark DataFrames

With SQLContext and a loaded local DataFrame, we create a Spark DataFrame:

sdf = sqlContext.createDataFrame(mtcars) 

sdf.printSchema()

#Displays the content of the DataFrame

sdf.show(5)

#Selecting columns
sdf.select('mpg').show(5)

#Filtering Data
Filter the DataFrame to only retain rows with mpg less than 18

sdf.filter(sdf['mpg'] < 18).show(5)

#Operating on Columns

#SparkR also provides a number of functions that can directly applied to columns for data processing and aggregation. The example below shows the use of basic arithmetic functions to convert lb to metric ton.
sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(6)
sdf.show(6)

#Grouping, Aggregation

#Spark DataFrames support a number of commonly used functions to aggregate data after grouping. For example we can compute the average weight of cars by their cylinders as shown below:
sdf.groupby(['cyl'])\
.agg({"wt": "AVG"})\
.show(5)

# We can also sort the output from the aggregation to get the most common cars
car_counts = sdf.groupby(['cyl'])\
.agg({"wt": "COUNT"})\
.sort("COUNT(wt)", ascending=False)\
.show(5)

#Running SQL Queries from Spark DataFrames

#A Spark DataFrame can also be registered as a temporary table in Spark SQL and registering a DataFrame as a table allows you to run SQL queries over its data. The sql function enables applications to run SQL queries programmatically and returns the result as a DataFrame.
# Register this DataFrame as a table.

sdf.registerTempTable("cars")

# SQL statements can be run by using the sql method

highgearcars = sqlContext.sql("SELECT gear FROM cars WHERE cyl >= 4 AND cyl <= 9")
highgearcars.show(6)
