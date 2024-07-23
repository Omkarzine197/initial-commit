#Reporting layer

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
currentdate = datetime.datetime.now().strftime("%Y-%m-%d")

spark=SparkSession.builder.appName("GOLDtoREPORTING_Revenue").getOrCreate()

#config_file_path = 'D://config.json'
#config_file_path = '/home/hadoop/config.json'
#
#def read_config_from_json(json_file):
#    with open(json_file, 'r') as file:
#        config = json.load(file)
#    return config
#
#config_data = read_config_from_json(config_file_path)

config_data={
  "tables": ["address","city","complaint","country","plan_postpaid","plan_prepaid","staff","subscriber"],
  "host": "jdbc:postgresql://database-1.c9886qoyaya5.ap-south-1.rds.amazonaws.com:5432/PROD",
  "username": "puser",
  "pwd": "ppassword",
  "driver": "org.postgresql.Driver",
  "bronze_layer_path": "s3://glueb101234/bronze_data/",
  "silver_layer_path": "s3://glueb101234/silver_data/",
  "gold_layer_path": "s3://glueb101234/gold_data/",
  "platinum_layer_path": "s3://glueb101234/report_data/",
  "sub_dtl_tgt_tbl": "subscriber_details",
  "cmp_dtl_tgt_tbl": "complaint_details",
  "revenue_tbl": "revenue_report"
}

# Access parameters from the config data
platinum_layer_path = config_data.get("platinum_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
revenue_tbl = config_data.get("revenue_tbl", "")

def read_parquet(spark,path):
    df=spark.read.format("parquet").load(path)
    return df


#read data 

df_sb=read_parquet(spark,gold_layer_path+'subscriber_details')

df_sb.createOrReplaceTempView("subscriber")

revenue_report=spark.sql("""
					select 
					SD.country as Country,
					count(SD.subscriberid) AS total_subscriber,
					sum(coalesce(pre_amount,pos_amount,0)) as total_revenue
					FROM subscriber SD
					group by SD.country,Active_flag having SD.Active_flag='A'
					""")


revenue_report.show()

#write Data : 
def write_data_parquet_fs(spark,df,path):
    df.write.format("parquet").save(path)
    print("Data Successfully return in FS") 

write_data_parquet_fs(spark, revenue_report, platinum_layer_path+revenue_tbl)