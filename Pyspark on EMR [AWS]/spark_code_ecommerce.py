#Importing Packages
from xmlrpc.client import DateTime
import pandas as pd
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

#User Defined Function to get Week from TimeStamp
def get_week(date):
	
	day=day(date)
	month=month(date)
	year=year(date)
	
	if (year%4 ==0): #Leap Year
		match(month):
			case 1: day=day+0;
			case 2: day=day+31;
			case 3: day=day+60;
			case 4: day=day+91;
			case 5: day=day+121;
			case 6: day=day+152;
			case 7: day=day+182;
			case 8: day=day+213;
			case 9: day=day+244;
			case 10: day=day+274;
			case 11: day=day+305;
			case 12: day=day+335;
		
		week = int(day/7+1)

	else:
		match(month):
			case 1: day=day+0;
			case 2: day=day+31;
			case 3: day=day+59;
			case 4: day=day+90;
			case 5: day=day+120;
			case 6: day=day+151;
			case 7: day=day+181;
			case 8: day=day+212;
			case 9: day=day+243;
			case 10: day=day+273;
			case 11: day=day+304;
			case 12: day=day+334;
		
		week = int(day/7+1)
	
	return week


#User Defined Function to Get Day Name
def get_day_name(date):
	
	day_name=dayofweek(date)
	
	match(day_name):
		case 1: day_name='Sunday';
		case 2: day_name='Monday';
		case 3: day_name='Thursday';
		case 4: day_name='Wednesday';
		case 5: day_name='Tuesday';
		case 6: day_name='Friday';
		case 7: day_name='Saturday';
	
	return day_name


#Starting Spark Session Builder
S3_DATA_SOURCE_PATH = "s3://ecommerceanalyticsbucket/data-source/Olist Dataset_public_dataset.csv"
S3_DATA_OUTPUT_PATH = "s3://ecommerceanalyticsbucket/data-output/"

spark = SparkSession.builder.appName('EcommerceAnalytics').enableHiveSupport().getOrCreate()

#RDD from CSV
rdd = spark.sparkContext.parallelize(S3_DATA_SOURCE_PATH)
print(rdd)

#Defining dataset schema for spark data frame
myschema = StructType(\
				List(
					StructField("Id",StringType(),True),
					StructField("order_status",StringType(),True),
					StructField("order_products_value",DoubleType(),True),
					StructField("order_freight_value",DoubleType(),True),
					StructField("order_items_qty",IntegerType(),True),
					StructField("customer_city",StringType(),True),
					StructField("customer_state",StringType(),True),
					StructField("customer_zip_code_prefix",IntegerType(),True),
					StructField("product_name_lenght",IntegerType(),True),
					StructField("product_description_lenght",IntegerType(),True),
					StructField("product_photos_qty",IntegerType(),True),
					StructField("review_score",IntegerType(),True),
					StructField("order_purchase_timestamp",DateTime(),True),
					StructField("order_aproved_at",DateTime(),True),
					StructField("order_delivered_customer_date",DateTime(),True)
				)
			)

#Reading dataset from S3 source folder
spark_df_with_schema = spark.read.format("csv") \
      .option(header='True', delimiter=',') \
      .schema(myschema) \
      .load(S3_DATA_SOURCE_PATH)

df = spark_df_with_schema


#Writing Parquet File on S3 output folder
df.write.option("header",True) \
 .parquet(S3_DATA_OUTPUT_PATH)


#Converting function to UDF
get_weekUDF = udf(lambda z: get_week(z),IntegerType())
get_day_nameUDF = udf(lambda z:get_day_name(z),StringType())

df.withColumn("Weekday", get_day_nameUDF(col("order_purchase_timestamp"))).show(truncate=False)
df.withColumn("Week", get_weekUDF(col("order_purchase_timestamp"))).show(truncate=False)

#Using Select with UDF
df.select(get_weekUDF(col("order_purchase_timestamp"))).show(truncate=False)

#Using UDF on SQL
spark.udf.register("get_weekUDF", get_week,IntegerType())
spark.udf.register("get_day_nameUDF", get_day_name,StringType())

#Creating Table
spark.sql("CREATE TABLE IF NOT EXISTS Olist (\
			Id STRING,\
			order_status STRING,\
			order_products_value DOUBLE,\
			order_freight_value DOUBLE,\
			order_items_qty INT,\
			customer_city STRING,\
			customer_state STRING,\
			customer_zip_code_prefix INT,\
			product_name_lenght INT,\
			product_description_lenght INT,\
			product_photos_qty INT, review_score INT,\
			order_purchase_timestamp TIMESTAMP,\
			order_aproved_at TIMESTAMP,\
			order_delivered_customer_date TIMESTAMP\
			)")

spark.sql("LOAD DATA LOCAL INPATH '~/AWS_Project2/olist_public_dataset.csv' INTO TABLE Olist")
spark.sql("SELECT * FROM Olist").show()

#Creating View
df.createOrReplaceTempView("Olist_Dataset")

#########################################################################################
#Daily Insights
#Total Sales groupby Day orderby City
spark.sql("select customer_city as City,\
			get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			sum(order_product_value) as Total Sales\
			from Olist_Dataset\
			groupby Weekday\
			orderby City").show(truncate=False)

#Total Sales groupby city orderby day
spark.sql("select customer_city as City,\
			get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			sum(order_product_value) as Total Sales\
			from Olist_Dataset\
			groupby customer_city\
			orderby Weekday").show(truncate=False)

#Total Sales groupby state orderby day 
spark.sql("select customer_state as State,\
			get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			sum(order_product_value) as Total Sales\
			from Olist_Dataset\
			groupby customer_state\
			orderby Weekday").show(truncate=False)

#Total number of orders groupby Day
spark.sql("select get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			count(order_product_value) as Total Orders\
			from Olist_Dataset\
			groupby Weekday").show(truncate=False)

#Total number of orders groupby City orderby Weekday
spark.sql("select customer_city as City,\
			get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			count(order_product_value) as Total Orders\
			from Olist_Dataset\
			groupby City\
			orderby Weekday").show(truncate=False)

#Total number of orders groupby State orderby Weekday
spark.sql("select customer_state as State,\
			get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			count(order_product_value) as Total Orders\
			from Olist_Dataset\
			groupby State\
			orderby Weekday").show(truncate=False)

#Average review score, average freight value, average order approval, and delivery time
spark.sql("select get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			order_status as Order_Status,\
			avg(review_score) as AVG_Review,\
			avg(freight_value) as AVG_Freight,\
			avg(order_status) as AVG_Approval,\
			order_delivered_customer_date as Delivered_Time\
			from Olist_Dataset\
			where Order_Status = 'delivered' or Order_Status = 'shipped' or Order_Status = 'invoiced'\
			groupby Weekday").show(truncate=False)

#The freight charges per city and total freight charges
spark.sql("select get_day_nameUDF(order_purchase_timestamp) as Weekday,\
			order_freigth_value as Freigth_Charge,\
			sum(order_freight_value) as Total_Freight_Charges\
			from Olist_Dataset\
			groupby Weekday").show(truncate=False)

#########################################################################################
#Weekly Insights
#Total Sales groupby Week orderby City
spark.sql("select customer_city as City,\
			get_weekUDF(order_purchase_timestamp) AS Week,\
			sum(order_product_value) AS Total Sales\
			from Olist_Dataset\
			groupby Week\
			orderby City").show(truncate=False)

#Total sales groupby city orderby week 
spark.sql("select customer_city as City,\
			get_weekUDF(order_purchase_timestamp) as Week,\
			sum(order_product_value) as Total Sales\
			from Olist_Dataset\
			groupby customer_city\
			orderby Week").show(truncate=False)

#Total sales orderby week groupby state
spark.sql("select customer_state as State,\
			get_weekUDF(order_purchase_timestamp) as Week,\
			sum(order_product_value) as Total Sales\
			from Olist_Dataset\
			groupby customer_state\
			orderby Week").show(truncate=False)

#Total number of orders groupby Week
spark.sql("select get_weekUDF(order_purchase_timestamp) as Week,\
			count(order_product_value) as Total Orders\
			from Olist_Dataset\
			groupby Week").show(truncate=False)

#Total number of orders groupby City orderby Week
spark.sql("select customer_city as City,\
			get_weekUDF(order_purchase_timestamp) as Week,\
			count(order_product_value) as Total Orders\
			from Olist_Dataset\
			groupby City\
			orderby Week").show(truncate=False)

#Total number of orders groupby State orderby Week
spark.sql("select customer_state as State,\
			get_weekUDF(order_purchase_timestamp) as Week,\
			count(order_product_value) as Total Orders\
			from Olist_Dataset\
			groupby State\
			orderby Week").show(truncate=False)

#Average review score, average freight value, average order approval, and delivery time
spark.sql("select get_weekUDF(order_purchase_timestamp) as Week,\
			order_status as Order_Status,\
			avg(review_score) as AVG_Review,\
			avg(freight_value) as AVG_Freight,\
			avg(order_status) as AVG_Approval,\
			order_delivered_customer_date as Delivered_Time\
			from Olist_Dataset\
			where Order_Status = 'delivered' or Order_Status = 'shipped' or Order_Status = 'invoiced'\
			groupby Week").show(truncate=False)

#The freight charges per city and total freight charges
spark.sql("select get_weekUDF(order_purchase_timestamp) as Week,\
			order_freigth_value as Freigth_Charge,\
			sum(order_freight_value) as Total_Freight_Charges\
			from Olist_Dataset\
			groupby Week").show(truncate=False)


#Hive connection
#CREATE TEMPORARY FUNCTION hiveUDTF AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode';


















