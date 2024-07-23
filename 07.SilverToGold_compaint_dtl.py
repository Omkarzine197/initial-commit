#processing layer
#preprocessing to processed layer 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark=SparkSession.builder.appName("SilverToGold_CmpDtl").getOrCreate()

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
table_list = config_data.get("tables", [])
silver_layer_path = config_data.get("silver_layer_path", "")
gold_layer_path = config_data.get("gold_layer_path", "")
cmp_dtl_tgt_tbl = config_data.get("cmp_dtl_tgt_tbl", "")

print(table_list)
print(gold_layer_path)
print(silver_layer_path)


#read from preprocessing transoform as per logic and store in processed 

def read_parquet(spark,path):
    df=spark.read.format("parquet").load(path)
    return df


#read data 

df_sb=read_parquet(spark,silver_layer_path+'subscriber')
df_cm=read_parquet(spark,silver_layer_path+'complaint')


#read from processed transform and store in report s3 

#DSL Approch
df_sbb=df_sb.selectExpr("sid","name")
c_test=df_cm.join(df_sbb, "sid", how="inner")

c_test.show(5)

#Give Alias
res_cmd=c_test.selectExpr("sid as subscriberId", "name as subscribername","cmp_id as complaintId","regarding as complaintReg","descr as description","sys_cre_date as com_cre_date","sys_upd_date as com_upd_date","status as status")

res_cmd.show(5)

#write Data : 
def write_data_parquet_fs(spark,df,path):
    df.write.format("parquet").save(path)
    print("Data Successfully return in FS") 

write_data_parquet_fs(spark, res_cmd, gold_layer_path+cmp_dtl_tgt_tbl)


