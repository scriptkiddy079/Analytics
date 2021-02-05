# from time import time
from time import time
import psycopg2
from PIL import Image
st = time()
conn = psycopg2.connect(host="production-16.svcolo.movoto.net", database="geo", user="analytics", password="igen",
                        port=5432)

query = "create table tmp.canopy_temp as SELECT cgr.geo_id as geo_id ,cgr.geo_type as geo_type,cc.rast as rast," \
        "cgr.clipped_line as clipped_line from tmp.city_geo_roads cgr left JOIN climate.canopy cc ON ST_Intersects(" \
        "cgr.clipped_line, cc.rast) "
cur = conn.cursor()
cur.execute(query)
et=time()
print("##############################################################################################################################################")
print("Total Time Taken: ",((et-st)/60)," minutes")
# proc_engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')
