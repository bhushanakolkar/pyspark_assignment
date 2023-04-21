from pyspark.sql.functions import when, regexp_replace,lower
from pyspark.sql import SparkSession

import os
os.chdir('/home/user/My_Notes/pyspark_')

spark = SparkSession.builder.appName('Mysql_connectivity')\
                    .config("spark.jars", "mysql-connector-java-8.0.28.jar")\
                    .getOrCreate()

df = spark.read.load('startup1.csv',format='csv',inferSchema='true',sep=',',header='true')

# Add new column named ‘startup_name_processed’ copy of an existing data column Startup_Name after removing spaces from this column
df = df.withColumn('startup_name_processed', when(df.Startup_Name.contains(' '), regexp_replace(df.Startup_Name,' ',''))\
                   .otherwise(df.Startup_Name))



# Convert all values in City Name column to complete lower case
df = df.withColumn('City', lower(df.City))


df.show(10)
df.printSchema()

# Upload result table into MySQL
df.write\
  .format("jdbc")\
  .option("driver","com.mysql.cj.jdbc.Driver")\
  .option("url", "jdbc:mysql://localhost:3306/db_pyspark")\
  .option("dbtable", "startup")\
  .option("user", "root")\
  .option("password", "Test@123")\
  .mode("overwrite")\
  .save()


spark.stop()