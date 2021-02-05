# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/geo.parquet
# hdfs://analytics-p05.svcolo.movoto.net:9000/tmp/trails.parquet
import os
from multiprocessing import Pool
import geopandas as gpd
import numpy as np
import pandas as pd
from geoalchemy2 import Geometry
from shapely import wkt
from shapely import geometry
from sqlalchemy import create_engine
from time import time

start = time()
engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')
trails_query = "select id as trails_id,geom as trails_geom,geom from poi.trails"

# trails_df = gpd.read_postgis(sql=trails_query, con=engine, geom_col="geom", crs='epsg:4326')
trails_df = pd.read_parquet("./trails.parquet")
trails_df["geom"] = trails_df["geom"].apply(wkt.loads)

trails_df = gpd.GeoDataFrame(trails_df, geometry="geom", crs='epsg:4326')
# trails_df = trails_df.set_geometry("geom", crs='epsg:4326', inplace=True)
# trails_df["trails_geom"] = trails_df["geom"].values
trails_df.rename(columns={"id": "trails_id"}, inplace=True)

# print(trails_df.head())
trails_df.reset_index()
print("Trails Loaded")


def process(geo_df):
    try:
        geo_df["geom_valid"] = geo_df["geom_valid"].apply(wkt.loads)
        geo_df = gpd.GeoDataFrame(geo_df, geometry="geom_valid", crs='epsg:4326')
        df = gpd.sjoin(geo_df, trails_df, how="inner", op="intersects", lsuffix="geographic", rsuffix="trails")

        print("join completed!!!!!!!!!!!")
        # df["trails_geom"] = df["trails_geom"].to_crs('epsg:4326')
        df.reset_index()
        # print(df.dtypes)
        proc_engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')
        df = df.drop(columns="index_trails")
        # df = df.set_crs(epsg=4326, inplace=False, allow_override=True)
        # print(df.head(20))
        # df.to_parquet("./result.parquet", mode="append")
        # df.to_sql()
        # df = df.set_geometry("trails_geom", crs='epsg:4326', inplace=False)
        # df["geom_valid"] = df["geom_valid"].to_crs('epsg:4326').astype(np.str)
        # df["trails_geom"] = df["trails_geom"].to_crs('epsg:4326').astype(np.str)
        # df = pd.DataFrame(data=df, index=False)
        # df.set_geometry("geom_valid", crs='epsg:4326', inplace=True)
        # df = df.set_crs(epsg=4326, inplace=False, allow_override=True)
        # print(df.crs)
        df = pd.DataFrame(df.drop(columns="geom_valid"))
        df.to_sql(con=proc_engine, name="all_geo_trails", schema="tmp", if_exists='append', index=False,
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
    geo_query = "select id as geo_id,geom_valid from movoto.geographic_boundary limit 100"
    # gpd.read_parquet()
    # geo_df = gpd.read_postgis(sql=geo_query, con=engine, geom_col="geom_valid", crs='epsg:4326')
    geo_df = pd.read_parquet("./geo.parquet")
    geo_df.rename(columns={"geom": "geom_valid", "id": "geo_id"}, inplace=True)
    print("Geography Boundary Loaded")
    num_processes = int(os.cpu_count())
    print("Total Cores using: ", num_processes)
    parallelProcessing(geo_df.loc[10:18, :], num_processes)


main()
