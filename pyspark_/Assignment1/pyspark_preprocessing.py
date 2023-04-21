import os
os.chdir('/home/user/My_Notes/pyspark_/')
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,regexp_replace


spark = SparkSession.builder.appName('pyspark_preprocessing').getOrCreate()

#Readding Datasets
startup_df = spark.read.load("startup.csv",format="csv",inferSchema="true",sep=",",header="true")
consumer_df = spark.read.load("consumerInternet.parquet",format="parquet",inferSchema="true",header="true")


print("Total count of Startup dataset:",startup_df.count())
print('Total count of consumer internet dataset:',consumer_df.count())

#Merging Datasets
final_df = startup_df.union(consumer_df)
print('Total count after merging two datasets:',final_df.count())




for column in final_df.columns:
    final_df = final_df.withColumn(column, when(final_df[column].contains('\\xc2\\xa0'), regexp_replace(final_df[column], '\\\\xc2\\\\xa0',''))\
                                   .when(final_df[column].contains(r'\\xc2\\xa0'), regexp_replace(final_df[column], '\\\\\\\\xc2\\\\\\\\xa0',''))\
                                   .otherwise(final_df[column]))
    

#Date column Cleaning
final_df = final_df.withColumn('Date', when(final_df['Date'].contains('//'), regexp_replace(final_df['Date'], '//','/'))\
                               .when(final_df['Date'].contains('.'), regexp_replace(final_df['Date'],'.20','/20'))\
                               .when(final_df.Date.contains('01/07/015'), regexp_replace(final_df.Date, '01/07/015','01/07/2015'))\
                               .when(final_df.Date.contains('05/072018'), regexp_replace(final_df.Date, '05/072018','05/07/2018'))\
                               .when(final_df.Date.contains('10/7/2015'), regexp_replace(final_df.Date, '10/7/2015','10/07/2015'))
                                .otherwise(final_df['Date']))


final_df = final_df.withColumn('Year',final_df.Date.substr(7,4))


#Amount_in_USD column cleaning
final_df = final_df.withColumn('Amount_in_USD', when(final_df['Amount_in_USD'].contains(','), regexp_replace(final_df['Amount_in_USD'], ',',''))\
                               .when(final_df['Amount_in_USD'].contains('xa0'), regexp_replace(final_df['Amount_in_USD'], 'xa0',''))\
                                .otherwise('0'))

final_df['Amount_in_USD'].cast('Integer')

#Dropping Remark column
final_df = final_df.drop('Remarks')

#cleaning Startup_Name column
final_df = final_df.withColumn('Investors_Name', when(final_df['Investors_Name'].contains(r'\\xc2\\xa0'), regexp_replace(final_df['Investors_Name'], '\\\\\\\\xc2\\\\\\\\xa0',''))\
                               .when(final_df.Investors_Name.contains('xc2xa0'), regexp_replace(final_df.Investors_Name,'xc2xa0',''))\
                                .when(final_df.Investors_Name.contains('xe2x80x99'), regexp_replace(final_df.Investors_Name,'xe2x80x99',''))\
                                .otherwise(final_df['Investors_Name']))


#cleaning Startup_Name column
final_df = final_df.withColumn('Startup_Name', when(final_df['Startup_Name'].contains('\\xe2\\x80\\x99'), regexp_replace(final_df['Startup_Name'], 'xe2\\\\x80\\\\x99',''))\
                               .when(final_df.Startup_Name.contains('xe2x80x99'), regexp_replace(final_df.Startup_Name,'xe2x80x99',''))\
                                .when(final_df.Startup_Name.contains(r'\\xe2\\x80\\x99'), regexp_replace(final_df.Startup_Name,'\\\\\\\\xe2\\\\\\\\x80\\\\\\\\x99',''))\
                                .otherwise(final_df['Startup_Name']))

#Saving data on local in clean_data folder
final_df.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save('clean_data')

spark.stop()

