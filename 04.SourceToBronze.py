from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark=SparkSession.builder.appName("SourceToBronze").getOrCreate()


#code to read json file from path 
#config_file_path = 'D://config.json'
#config_file_path = '/home/hadoop/config.json'

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
table_list = config_data.get("tables", [])
host = config_data.get("host", "")
username = config_data.get("username", "")
pwd = config_data.get("pwd", "")
driver = config_data.get("driver", "")
bronze_layer_path = config_data.get("bronze_layer_path", "")

print(table_list)
print(host)
print(username)
print(pwd)
print(driver)
print(bronze_layer_path)

#read data function from RDBMS
def read_data_from_rdbms(spark,host,username,pwd,driver,table_name):
    df=spark.read.format("JDBC").option("url",host).option("user", username).option("password", pwd)\
        .option("driver", driver).option("dbtable", table_name).load()
    return df

def write_data_fs(spark,df,path,delim=',',header="true"):
    df.write.format("CSV").option("delimiter",delim).option("header",header).save(path)
    print("Data Successfully return in FS")

for table in table_list:
    print("Data Load Started for ",table)
    df=read_data_from_rdbms(spark, host, username, pwd, driver, table)
    df.show()
    write_data_fs(spark, df, bronze_layer_path+table,header="true")

print("********Job Successfully Completed**********")