# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/geo.parquet
# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/trails.parquet
import os
from multiprocessing import Pool
from time import time

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt
from sqlalchemy import create_engine

num_processes = int(os.cpu_count())
print("Total Cores using: ", num_processes)
start = time()
engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')

import pyarrow as pyarr


###########################################################################################################################################################################
def process_roads(df):
    df["geom"] = df["geom"].apply(wkt.loads)
    return df


# trails_df = gpd.read_postgis(sql=trails_query, con=engine, geom_col="geom", crs='epsg:4326')
roads_df = pd.read_parquet("./roads.parquet")
print("Roads data Loaded", "Moving For Transformation")
roads_df_split = np.array_split(roads_df, num_processes)

pool = Pool(num_processes)
roads_df = gpd.GeoDataFrame(pd.concat(pool.map(process_roads, roads_df_split), ignore_index=True), geometry="geom",
                            crs='epsg:4326')
pool.close()
pool.join()
# roads_df["geom"] = roads_df["geom"].apply(wkt.loads)

# roads_df = gpd.GeoDataFrame(roads_df, geometry="geom", crs='epsg:4326')
# trails_df = trails_df.set_geometry("geom", crs='epsg:4326', inplace=True)
# trails_df["trails_geom"] = trails_df["geom"].values
roads_df["roads_geom"] = roads_df["geom"]
roads_df.rename(columns={"linear_id": "roads_id"}, inplace=True)

# print(trails_df.head())
roads_df.reset_index()
print("Roads Transformed")


def process(geo_df):
    try:
        geo_df["geom_valid"] = geo_df["geom_valid"].apply(wkt.loads)
        geo_df = gpd.GeoDataFrame(geo_df, geometry="geom_valid", crs='epsg:4326')
        df = gpd.sjoin(geo_df, roads_df, how="left", op="intersects", lsuffix="geo", rsuffix="road")

        print("join completed!!!!!!!!!!!")
        # df["trails_geom"] = df["trails_geom"].to_crs('epsg:4326')
        df.reset_index()
        # print(df.dtypes)
        proc_engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')
        df = df.drop(columns="index_road")
        df["clipped_line"] = gpd.GeoDataFrame(df.loc[:, ["geo_id", "geo_type", "geom_valid"]], geometry="geom_valid",
                                              crs='epsg:4326').intersection(
            gpd.GeoDataFrame(df.loc[:, ["roads_id", "roads_geom"]], geometry="roads_geom", crs='epsg:4326'))
        df = df.drop(columns=["roads_geom", "geom_valid", "roads_id"])
        df.set_geometry("clipped_line", crs='epsg:4326', inplace=True)

        # df = df.set_crs(epsg=4326, inplace=False, allow_override=True)
        # print(df.head(20))
        # df.to_parquet("./result.parquet", mode="append")
        # df.to_sql()
        # df = df.set_geometry("trails_geom", crs='epsg:4326', inplace=False)
        # df["trails_geom"] = df["trails_geom"].to_crs('epsg:4326').astype(np.str)
        # df = pd.DataFrame(data=df, index=False)
        # df.set_geometry("geom_valid", crs='epsg:4326', inplace=True)
        # df = df.set_crs(epsg=4326, inplace=False, allow_override=True)
        # print(df.crs)
        df["clipped_line"] = df["clipped_line"].to_crs('epsg:4326').apply(wkt.dumps)
        df = pd.DataFrame(df)
        df.to_sql(con=proc_engine, name="all_geo_roads", schema="tmp", if_exists='append', index=False,
                  chunksize=10000)
        print("Insertion Completed")
    except BaseException as e:
        print(e)
        exit(-1)


def parallelProcessing(geo_df, n_cores):
    df_split = np.array_split(geo_df, n_cores)
    pool = Pool(n_cores)
    pool.map(process, df_split)
    # pool.apply(process, df_split, trails_df)
    pool.close()
    pool.join()
    end = time()
    total_time = str(((end - start) / 60))
    print(
        "#######################################################################################################################################################################")
    print("Total Time Taken: ", total_time + " minutes")


def main():
    geo_df = pd.read_parquet("./geo.parquet")
    print("Geography Boundary data Loaded", "Moving For Transformation")
    geo_df.rename(columns={"geom": "geom_valid", "id": "geo_id", "type": "geo_type"}, inplace=True)
    print("Geography Boundary Transformed")
    parallelProcessing(geo_df, num_processes)


main()
