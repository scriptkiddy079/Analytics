import pandas as pd
from pyhive import hive
import json
import os
import numpy as np
from sqlalchemy import create_engine
from multiprocessing import Pool

p05_conn = hive.Connection(host="analytics-p05.svcolo.movoto.net", port="10000", username="pray")
dev_engine = create_engine('postgresql://analytics:igen@10.255.1.211:5432/geo')

prod_engine = create_engine('postgresql://analytics:igen@10.77.2.36:5432/geo')


def func(x):
    rec = json.loads(x.metrics)
    x["geo_id"] = int(rec["geo_id"])
    x["geo_type"] = str(rec["geo_type"])
    x["multi"] = int(rec["property_type"].get("multi", 0))
    x["single"] = int(rec["property_type"].get("single", 0))
    x["condo"] = int(rec["property_type"].get("condo", 0))
    x["land"] = int(rec["property_type"].get("land", 0))
    x["owner"] = int(rec["owner_details"].get("owner", 0))
    x["rental"] = int(rec["owner_details"].get("rental", 0))
    x["total_units"] = x["owner"] + x["rental"]
    x["owner_pct"] = round(x["owner"] / x["total_units"], 4)
    x["house_age_avg"] = int(rec["house_age_avg"])
    x["sqft_avg"] = float(rec["sqft_avg"])
    x["lot_size_avg"] = float(rec["lot_size_avg"])
    x["house_age"] = str(rec["house_age"])
    x["lot_size"] = str(rec["lot_size"])
    return x


def process(df):
    df = df.apply(lambda x: func(x), axis=1)
    df = df.drop("metrics", axis=1)
    df.set_index("geo_id")
    df.to_sql("housing_aggs", schema="market", if_exists='append', con=dev_engine, index=False)


def parallelProcessing(df, n_cores):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    pool.map(process, df_split)
    pool.close()
    pool.join()


# geo_id, geo_type, total_units, multi, single, condo, land, owner_pct, house_age_avg, sqft_avg, lot_size_avg, house_age, lot_size
def main():
    home_type_city = "SELECT metrics FROM geomart.home_type_city"
    home_type_county = "SELECT metrics FROM geomart.home_type_county"
    home_type_neighbor = "SELECT metrics FROM geomart.home_type_neighbor"
    home_type_zip = "SELECT metrics FROM geomart.home_type_zip"

    city = pd.read_sql(home_type_city, p05_conn)
    print("home_type_city completed")
    county = pd.read_sql(home_type_county, p05_conn)
    print("home_type_county completed")
    neighbor = pd.read_sql(home_type_neighbor, p05_conn)
    print("home_type_neighbor completed")
    zip = pd.read_sql(home_type_zip, p05_conn)
    print("home_type_zip completed")

    df = pd.concat([city, county, neighbor, zip])
    parallelProcessing(df, int(os.cpu_count()))


main()
