from rasterio._io import CRS
from sqlalchemy import create_engine
import geopandas as gpd
import pandas as pd
import numpy as np
from rasterio.io import MemoryFile
from earthpy import epsg
import georasters as gr
import multiprocessing as mp
import time

num_process = mp.cpu_count() - 1


def makeTupleList(min_id, max_id, num_cores):
    step = (max_id - min_id) // num_cores
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


################################################################################################################################################################################


def process_city_geo(lst):
    query = "select * from tmp.city_geo_roads where geo_id>='" + str(lst[0]) + "' and geo_id<'" + str(lst[1]) + "'"
    p16_engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')
    return gpd.read_postgis(query, con=p16_engine, geom_col="clipped_line", crs='epsg:4326')


def read_city_geo():
    pool = mp.Pool(num_process)
    min_geo_id = 57
    max_geo_id = 253701
    city_geo_list = makeTupleList(min_geo_id, 120, num_process)

    res = []
    for x in city_geo_list:
        res.append(pool.apply_async(process_city_geo, args=(x,)))
    city_geo_df_list = []
    for x in res:
        city_geo_df_list.append(x.get())
    pool.close()
    pool.join()
    return city_geo_df_list


def to_georaster(x):
    with MemoryFile(x) as memfile:
        ds = memfile.open()
        return gr.GeoRaster(ds.dataset_mask(), geot=ds.get_transform(), nodata_value=ds.meta['nodata'],
                            projection=epsg[str(ds.crs.to_epsg())], datatype=ds.meta["dtype"])


def process_canopy(lst):
    query = "select rid,ST_Envelope(rast) as geom, ST_AsGDALRaster(rast, 'GTiff') as tiff FROM climate.canopy where rid>='" + str(
        lst[0]) + "' and rid<'" + str(lst[1]) + "'"
    p16_engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')
    df = gpd.read_postgis(query, con=p16_engine, geom_col="geom", crs='epsg:4326')
    df["tiff"] = df["tiff"].apply(bytes)
    df["tiff"] = df["tiff"].apply(lambda x: to_georaster(x))
    return df


def read_canopy():
    pool = mp.Pool(num_process)
    min_rid = 1
    max_rid = 182410
    canopy_list = makeTupleList(min_id=min_rid, max_id=100, num_cores=num_process)
    res = []
    for x in canopy_list:
        res.append(pool.apply_async(process_canopy, args=(x,)))
    canopy_df_list = []
    for x in res:
        canopy_df_list.append(x.get())
    pool.close()
    pool.join()
    return canopy_df_list


# ----------------------------------------------------------------
st = time.time()
canopy_df = gpd.GeoDataFrame(pd.concat(read_canopy(), ignore_index=True), geometry="geom", crs='epsg:4326')
et_cd = time.time()
print("Time taken to complete the Reading of Canopy: ", str(((et_cd - st) / 60)), " minutes")
canopy_df.info()


# ----------------------------------------------------------------

def get_mean_stats(x):
    # print("Tiff data", x.tiff)
    # print("geom data", x.geom)
    print(type(x.tiff))
    if pd.isna(x.tiff) and pd.isna(x.clipped_line):
        return 0.0
    df = x.tiff.stats(
        shp=gpd.GeoDataFrame(x.clipped_line, geometry="geometry", index=[x.index_canopy], columns=["geometry"],
                             crs='epsg:4326'),
        raster_out=True)
    print(df.dtypes)

    return df.mean


def process_join(df_geo):
    # print(canopy_df.head(20))
    cgdf = gpd.GeoDataFrame(df_geo, geometry="clipped_line", crs='epsg:4326')
    df = gpd.sjoin(cgdf, canopy_df, how="left", op="intersects", lsuffix="city_geo", rsuffix="canopy")
    # df = df.drop(columns=["index_city_geo", "rid"])
    print("######################## Join Completes #################################")
    # print(df.loc[:, ["geo_id", "geo_type"]].head(20))
    print(df.columns)
    print(df.dtypes)
    print(df["tiff"].head(20))
    # df["mean"] = df[["tiff", "geom"]].apply(lambda x: get_mean_stats(x))
    df_test = df.apply(lambda x: get_mean_stats(x), axis=1)
    print(df_test.dtypes)
    print(df_test.columns)
    return df


if __name__ == '__main__':
    st = time.time()
    ######################
    city_geo_df = gpd.GeoDataFrame(pd.concat(read_city_geo(), ignore_index=True), geometry="clipped_line",
                                   crs='epsg:4326')
    et_cgd = time.time()
    print("Time taken to complete the reading City_Geo: ", str(((et_cgd - st) / 60)), " minutes")
    city_geo_df.info()
    ######################
    # canopy_df = pd.concat(read_canopy(), ignore_index=True)
    # et_cd = time.time()
    # print("Time taken to complete the Reading of Canopy: ", str(((et_cd - st) / 60)), " minutes")
    # canopy_df.info()

    ######################### Move For Join ##########################################################################################################
    pool = mp.Pool(num_process)
    cg_split = np.array_split(city_geo_df, num_process)
    df = pd.DataFrame(pd.concat(pool.map(process_join, cg_split), ignore_index=True))
    # df = df.drop(columns=["index_city_geo", "rid", "geom", "tiff"])
    print("##########################################################################################")
    print(df.dtypes)
    print(df.columns)
    print(df.head(20))
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    # print(df["mean"].head(20))
    # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    # df.info()
