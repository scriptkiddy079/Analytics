# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/geo.parquet
# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/trails.parquet

import os

from pyspark.sql import SparkSession

###############################################################################################################################
os.environ["spark.pyspark.python"] = "/usr/bin/python3"
os.environ["spark.pyspark.driver.python"] = "/usr/bin/python3"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["SPARK_HOME"] = "/home/prabhat/.local/lib/python3.6/site-packages/pyspark"
os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"
os.environ["JAVA_HOME"] = "/usr"
os.environ["geospark.global.charset"] = "utf8"

spark = SparkSession.builder.config("spark.jars.packages",
                                    "org.postgresql:postgresql:42.2.15").config(
    "spark.pyspark.python", "/usr/bin/python3").config("spark.pyspark.driver.python", "/usr/bin/python3").config(
    "spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s").config("spark.driver.memory", "12g") \
    .master("local[*]").appName("process").getOrCreate()

##############################################################################################################################

roads_df = spark.read.parquet("hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/roads.parquet")
roads_df.createOrReplaceTempView("roads")
roads_df = spark.sql("select * from roads limit 1000")
roads_df.write.parquet("./roads.parquet", mode="overwrite", compression="snappy")
print("Roads Loaded")

# geo_df = spark.read.parquet("hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/geo.parquet").select("id", "type", "geom")
# geo_df.createOrReplaceTempView("geo")
# geo_df = spark.sql("select id,geom from geo where type='CITY' and geom is not null")
# geo_df.write.parquet("./geo.parquet", mode="overwrite", compression="snappy")
print("Geography Boundary Loaded")
# parallelProcessing(geo_df, int(os.cpu_count()))
