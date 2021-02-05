from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


pairs = [("spark.serializer", "org.apache.spark.serializer.KryoSerializer")]

num_partitions = "32"

url = "jdbc:postgresql://production-16.svcolo.movoto.net:5432/geo"


def process(spark):
    query_roads = "(select linear_id,ST_AsText(geom) as geom from poi.roads) as r"
    db_roads = {"user": "analytics", "password": "igen", "driver": "org.postgresql.Driver",
                "numPartitions": num_partitions, "partitionColumn": "linear_id", "lowerBound": "11020229727",
                "upperBound": "11015481676123"}

    df_roads = spark.read.jdbc(url=url, table=query_roads, properties=db_roads).repartition(int(num_partitions),"linear_id","geom")
    df_roads.write.parquet(path="hdfs:////tmp/roads.parquet", mode="overwrite")
    ##################################################################################################################################################################
    # query_canopy = "(select rid,rast from climate.canopy) as r"
    # db_canopy = {"user": "analytics", "password": "igen", "driver": "org.postgresql.Driver",
    #              "numPartitions": num_partitions, "partitionColumn": "rid", "lowerBound": "1",
    #              "upperBound": "182410"}
    # df_canopy = spark.read.jdbc(url=url, table=query_canopy, properties=db_canopy)
    # df_canopy.write.parquet(path="hdfs:////tmp/canopy.parquet", mode="overwrite")


if __name__ == '__main__':
    conf = SparkConf().setAll(pairs)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print(
        "########################################################################################################################################################################")
    print("SPARK LOADED : ")
    process(spark)
