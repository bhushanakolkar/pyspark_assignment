import os
os.chdir('/home/user/My_Notes/pyspark_/clean_data/')
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()  
parser.add_argument('-f','--source_file', help= "Path of source file", type = str)
parser.add_argument('-d','--destination_directory', help = 'Path of Directory in which output is saved', type = str)  
args = parser.parse_args()  


spark = SparkSession.builder.appName('pyspark_assignment').getOrCreate()

data_df = spark.read.load(args.source_file,format='csv',inferSchema='true',sep=',',header='true')

data_df.createOrReplaceTempView('startup')

# How many startups are there in Pune City?
spark.sql('select count(distinct Startup_Name)as startups_in_pune from startup where lower(City) like "%pune%"')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Q1')

# How many startups in Pune got their Seed/ Angel Funding?
spark.sql('select count(distinct Startup_Name) as cnt from startup where lower(InvestmentnType) like "%%seed%/%%a%" and lower(City) like "%pune%"')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Q2')

# What is the total amount raised by startups in Pune City? 
spark.sql('select sum(Amount_in_USD) as total_amount from startup where lower(City) like "%pune%"')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Q3')

# What are the top 5 Industry_Vertical which has the highest number of startups in India?
spark.sql('select Industry_Vertical,count(distinct Startup_Name) as cnt from startup where Industry_Vertical<>"nan" group by Industry_Vertical order by cnt \
desc limit 5')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Q4')

#Find the top Investor(by amount) of each year.
spark.sql('with cte as \
          (select Investors_Name, Year, total_amount,dense_rank() over(partition by Year order by total_amount desc) as rnk  from \
          (select Investors_Name, Year, sum(Amount_in_USD) as total_amount from startup group by Year, Investors_Name) where Investors_Name<>"N/A") \
          select Investors_Name, Year, total_amount from cte where rnk=1 order by Year')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Q5')

#Find the top startup(by amount raised) from each city?
spark.sql('with cte as \
(Select Startup_Name,case when instr(City,"/")>0 then trim(substr(City,1,instr(City,"/")-1 )) else City end as City, Amount_in_USD from startup), \
cte1 as \
(select Startup_Name,City, sum(Amount_in_USD) as total_amount from cte group by City, Startup_Name), \
cte2 as \
(select Startup_Name,City, total_amount, row_number() over(partition by City order by total_amount desc) as rnk from cte1) \
select Startup_Name,City,total_amount from cte2 where rnk=1')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Bonus_1')

# Which SubVertical had the highest growth(in number of startups) over the years?
spark.sql('select SubVertical,count(distinct Startup_Name) as cnt from startup where SubVertical<>"nan" group by SubVertical order by cnt desc limit 1')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Bonus_2')

# Which SubVertical had the highest growth(in funding) over the years?
spark.sql('select SubVertical,sum(Amount_in_USD) as total_amount from startup where SubVertical<>"nan" group by SubVertical order by total_amount desc limit 1')\
.coalesce(1).write.format('csv').option('header','true').mode("overwrite").save(args.destination_directory+'/Bonus_3')


spark.stop()