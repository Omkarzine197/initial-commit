
from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import desc

spark=SparkSession.builder.appName("DedupSubScriberDetails").getOrCreate()

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
  "platinum_layer_path": "s3://glueb101234/platinum_data/",
  "sub_dtl_tgt_tbl": "subscriber_details",
  "cmp_dtl_tgt_tbl": "complaint_details",
  "revenue_tbl": "revenue_report",
  "delta_path": "s3://glueb101234/delta_data/"
}

# Access parameters from the config data
table_list = config_data.get("tables", [])
bronze_layer_path = config_data.get("bronze_layer_path", "")
silver_layer_path = config_data.get("silver_layer_path", "")
sub_dtl_tgt_tbl = config_data.get("sub_dtl_tgt_tbl", "")
delta_path = config_data.get("delta_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")

print(table_list)
print(silver_layer_path)
print(bronze_layer_path)
print(sub_dtl_tgt_tbl)


#read from preprocessing transoform as per logic and store in processed 

def read_parquet(spark,path):
    df=spark.read.format("parquet").load(path)
    return df


#read data 

df_sb=read_parquet(spark,gold_layer_path+sub_dtl_tgt_tbl+"/")

res=df_sb.withColumn("new_upd",F.when(df_sb.update_date.isNull(),F.to_timestamp(F.lit("1970-01-01 00:00:00 "),format="yyyy-MM-dd HH:mm:SS")).otherwise(df_sb.update_date)).drop("update_date")


f=res.select("*").withColumnRenamed("new_upd","update_date")

res1=f.withColumn("rn", F.row_number().over(Window.partitionBy("subscriberId").orderBy(desc("update_date"))))

res2=res1.filter(res1.rn == 1).drop("rn")

res2.show(30)

#write Data : 
def write_data_parquet_fs(spark,df,path):
    df.write.format("parquet").mode("overwrite").save(path)
    print("Data Successfully return in FS") 

write_data_parquet_fs(spark, res2, gold_layer_path+sub_dtl_tgt_tbl)

print("subscriber details de-duplication finished successfully and data pushed into final table")

spark.stop()
