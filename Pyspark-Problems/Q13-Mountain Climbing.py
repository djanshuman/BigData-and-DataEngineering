from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json
spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mountain_info, mountain_climbers):
	# Write code here
    
    joinedDF= mountain_info.join(mountain_climbers,
                            mountain_info.name == mountain_climbers.mountain_name,"inner")
    
    
    window_spec= W.partitionBy("mountain_name").orderBy(F.desc("climb_date"))
    from pyspark.sql.functions import col  
    
    result_df = mountain_climbers.withColumn("rnk",
                    F.rank().over(window_spec)).select(
                        "climb_date",
                        "climb_time",
                        "climber_name",
                        "mountain_name").where(col("rnk")==1)
            
    return result_df
