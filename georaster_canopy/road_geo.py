# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/geo.parquet
# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/trails.parquet
import os
from multiprocessing import Pool
import multiprocessing as mp
# from time import time
import time
import psutil
import geopandas_postgis as gpdp
import geopandas as gpd
import numpy as np
import pandas as pd
from geoalchemy2 import Geometry
from shapely import wkt
from shapely.geometry import LineString, MultiLineString, GeometryCollection
from sqlalchemy import create_engine
from itertools import product
import os

from pyspark.sql import SparkSession

num_processes = mp.cpu_count() - 1
print("Total Cores using: ", num_processes)


def check_and_pause(sec):
    memory_avail = psutil.virtual_memory().available / (1024 * 1024)
    cpu_usage = psutil.cpu_percent()
    while memory_avail <= 3072.00 and cpu_usage >= 85:
        print("Going for 1 s sleep")
        time.sleep(sec)
        memory_avail = psutil.virtual_memory().available / (1024 * 1024)
        cpu_usage = psutil.cpu_percent()


#######################################################################################################################################################
def makeTupleList(min_id, max_id, total, num_cores):
    step = total // num_cores
    id_range = []
    start = min_id
    end = min_id + step
    for x in range(num_cores):
        if x != num_cores - 1:
            id_range.append([start, end])
            start = end
            end += step
        else:
            id_range.append([start, max_id + 1])
    return id_range


#######################################################################################################################################################
def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]


def toPandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand


def spark_job():
    os.environ["spark.pyspark.python"] = "/usr/local/bin/python3.6"
    os.environ["spark.pyspark.driver.python"] = "/usr/local/bin/python3.6"
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"
    os.environ["SPARK_HOME"] = "/home/pkmishra/.local/lib/python3.6/site-packages/pyspark"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"
    os.environ["JAVA_HOME"] = "/usr"
    os.environ["geospark.global.charset"] = "utf8"

    spark = SparkSession.builder.config("spark.jars.packages",
                                        "org.postgresql:postgresql:42.2.15").config(
        "spark.pyspark.python", "/usr/bin/python3").config("spark.pyspark.driver.python", "/usr/bin/python3").config(
        "spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s").config("spark.driver.memory", "32g").config(
        "spark.driver.maxResultSize", "4G") \
        .master("local[*]").appName("process").getOrCreate()

    base_df = spark.read.parquet("hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/roads.parquet")

    # base_df.write.parquet("./roads.parquet", mode="overwrite", compression="snappy")

    print("Roads Loaded")

    geo_df = spark.read.parquet("hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/geo.parquet").select("id", "type",
                                                                                                      "geom")
    geo_df.createOrReplaceTempView("geo")
    geo_df = spark.sql("select * from geo where type = 'CITY' and geom is not null")

    # geo_df.write.parquet("./geo.parquet", mode="overwrite", compression="snappy")
    # print("Geography Boundary Loaded")
    # parallelProcessing(geo_df, int(os.cpu_count()))
    pandas_base_df = toPandas(base_df, n_partitions=num_processes)
    pandas_geo_df = toPandas(geo_df, n_partitions=num_processes)
    # pandas_base_df = base_df.toPandas()
    # pandas_geo_df = geo_df.toPandas()
    spark.stop()
    return pandas_base_df, pandas_geo_df


# roads_df, geo_df = spark_job()
# print("Roads Data")
# print(roads_df.head(20))
# print("Geo Data")
# print(geo_df.head(20))
# exit(0)

###############################################################################################################################################################################################################

start = time.time()
engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')


###########################################################################################################################################################################
def process_geos(df):
    df["geom_valid"] = df["geom_valid"].apply(wkt.loads)
    return df


def query_roads(x):
    query = "select linear_id as id,geom from poi.roads where geom is not null and linear_id>='" + str(
        x[0]) + "' and linear_id<'" + str(x[1]) + "'"
    roads_df = gpd.read_postgis(query, con=engine, geom_col="geom", crs='epsg:4326', index_col="id")
    return roads_df


####################################################################################################################################################
geo_df = pd.read_parquet("./geo.parquet")
print("Geography Boundary data Loaded", "Moving For Transformation")
geo_df.rename(columns={"geom": "geom_valid", "id": "geo_id", "type": "geo_type"}, inplace=True)

#####################################################################################################################################################

geo_df_split = np.array_split(geo_df, num_processes)
pool = Pool(num_processes)
geo_df = gpd.GeoDataFrame(pd.concat(pool.map(process_geos, geo_df_split), ignore_index=True), geometry="geom_valid",
                          crs='epsg:4326')
pool.close()
pool.join()
print("Geography Boundary Transformed")


#####################################################################################################################################################

# roads_df["geom"] = roads_df["geom"].apply(wkt.loads)
# roads_df = gpd.GeoDataFrame(roads_df, geometry="geom", crs='epsg:4326')

#####################################################################################################################################################

# roads_df.reset_index()


def transform_intersect(x):
    if x is None:
        return MultiLineString()
    if isinstance(x, GeometryCollection):
        mul = []
        for g in x:
            if isinstance(x, MultiLineString):
                return x
            if isinstance(g, LineString):
                mul.append(g)
        return MultiLineString(mul)

    if isinstance(x, LineString):
        return MultiLineString([x])


def process(roads_df):
    try:
        roads_df["geom"] = roads_df["geom"].apply(wkt.loads)
        roads_df = gpd.GeoDataFrame(roads_df, geometry="geom", crs='epsg:4326')
        roads_df["roads_geom"] = roads_df["geom"]
        roads_df.rename(columns={"linear_id": "roads_id"}, inplace=True)
        print("Roads Transformed")
        check_and_pause(1)
        print("join Started")
        df = gpd.sjoin(roads_df, geo_df, how="right", op="intersects", lsuffix="road", rsuffix="geo")
        print("join completed!!!!!!!!!!!")
        # df["trails_geom"] = df["trails_geom"].to_crs('epsg:4326')
        df.reset_index()
        print("Before Intersection Column: ")
        print(df.dtypes)
        proc_engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')

        df["clipped_line"] = gpd.GeoDataFrame(df.loc[:, ["geo_id", "geom_valid"]],
                                              geometry="geom_valid",
                                              crs='epsg:4326').intersection(
            gpd.GeoDataFrame(df.loc[:, ["roads_id", "roads_geom"]], geometry="roads_geom", crs='epsg:4326'))

        df.drop(columns="index_road", inplace=True)
        df = df.set_geometry("clipped_line", crs='epsg:4326')
        print("After Intersection Column: ")
        print(df.dtypes)
        df.drop(columns=["roads_geom", "geom_valid", "roads_id"], inplace=True)
        df["clipped_line"] = df["clipped_line"].to_crs('epsg:4326').apply(lambda x: transform_intersect(x))
        df.to_postgis(con=proc_engine, name="city_geo_roads", schema="tmp", if_exists='append', index=False,
                      dtype={"clipped_line": Geometry("MULTILINESTRING", srid=4326)})
        print("Insertion Completed")
    except OSError as oe:
        print("Got OS Error!!!!!!!!!!! ")
        time.sleep(600)
        print("restarting this process")
        process(roads_df)
    except BaseException as e:
        print(e)
        exit(-1)


def parallelProcessing(roads_df, n_cores):
    print("Entered for Parallel Processing!!!!!!!")
    df_split = np.array_split(roads_df, n_cores)
    pool = Pool(n_cores)
    res = []
    cnt = 0
    for x in df_split:

        cnt += 1
        check_and_pause(1)
        try:
            if cnt > n_cores // 2:
                check_and_pause(1)
            res.append(pool.apply_async(process, args=(x,)))

        except OSError as oe:
            print("Got OS Error!!!!!!!!!!! ")
            time.sleep(600)
            print("restarting this process")
            res.append(pool.apply_async(process, args=(x,)))
    for x in res:
        x.wait()
    pool.close()
    pool.join()


def main():
    roads_df = pd.read_parquet("./roads.parquet")
    print("Roads data Loaded!!!!!!!!!!!!!!!!!!!!!!!")
    print("Moving For Transformation!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    parallelProcessing(roads_df, num_processes)
    end = time.time()
    total_time = str(((end - start) / 60))
    print(
        "#######################################################################################################################################################################")
    print("Total Time Taken: ", total_time + " minutes")


main()
