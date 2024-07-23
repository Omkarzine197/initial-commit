#processing layer
#preprocessing to processed layer 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

spark=SparkSession.builder.appName("SilverToGold_SubDtl").getOrCreate()

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
sub_dtl_tgt_tbl = config_data.get("sub_dtl_tgt_tbl", "")

print(table_list)
print(gold_layer_path)
print(silver_layer_path)
print(sub_dtl_tgt_tbl)


#read from preprocessing transoform as per logic and store in processed 

def read_parquet(spark,path):
    df=spark.read.format("parquet").load(path)
    return df


#read data 

df_sb=read_parquet(spark,silver_layer_path+'subscriber')
df_ad=read_parquet(spark,silver_layer_path+'address')
df_ct=read_parquet(spark,silver_layer_path+'city')
df_cn=read_parquet(spark,silver_layer_path+'country')
df_ppr=read_parquet(spark,silver_layer_path+'plan_postpaid')
df_ppo=read_parquet(spark,silver_layer_path+'plan_prepaid')

#Apply Logic
test=df_sb.join(df_ad,"add_id", how="left").join(df_ct,"ct_id", how="left").join(df_cn, "cn_id", how="left")
test1=test.join(df_ppr, df_ppr.plan_id == test.prepaid_plan_id, how= "left").drop("add_id").drop("ct_id").drop("cn_id").drop("plan_id").withColumnRenamed("plan_desc","pre_plan_desc").withColumnRenamed("amount","pre_amount")

test2=test1.join(df_ppo, df_ppo.plan_id == test1.postpaid_plan_id, how= "left").drop("plan_id").withColumnRenamed("plan_desc","pos_plan_desc").withColumnRenamed("amount","pos_amount")

#Give alias

res=test2.selectExpr("sid as subscriberid", "name as subscribername","mob as contactnumber","email as emailid","street as address","ct_name as city","cn_name as country","sys_cre_date as create_date","sys_upd_date as update_date","active_flag as active_flag","pre_plan_desc as prepaid_desc","pre_amount as pre_amount","pos_plan_desc as postpaid_desc","pos_amount as pos_amount")

res.show(5)


#write Data : 
def write_data_parquet_fs(spark,df,path):
    df.write.format("parquet").save(path)
    print("Data Successfully return in FS") 

write_data_parquet_fs(spark, res, gold_layer_path+sub_dtl_tgt_tbl)
