from rasterio._io import CRS
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from rasterio.io import MemoryFile
from earthpy import epsg
import georasters as gr

# ST_AsGDALRaster(rast, 'GTiff')
proc_engine = create_engine('postgresql://analytics:igen@production-16.svcolo.movoto.net:5432/geo')
query = "SELECT rid,ST_AsGDALRaster(rast, 'GTiff') as tiff FROM climate.canopy limit 10"
df = pd.read_sql(query, con=proc_engine)
print(type(df["tiff"].tolist()[0]))
print(df["tiff"].tolist()[0])
df["tiff"] = df["tiff"].apply(bytes)
# df.to_csv("temp.csv", index=False)

print(df.dtypes)
print(df.columns)
with MemoryFile(df["tiff"].tolist()[1]) as memfile:
    ds = memfile.open()

# print(ds.get_tag_item())
print(ds.get_gcps())
print(ds.meta)
print("Type of meta: ", type(ds.meta))
print(ds.dataset_mask())
fill_value = -1e10
nodata = 255.0
print(np.ma.masked_array(ds.dataset_mask(), mask=ds.dataset_mask() == nodata,
                         fill_value=fill_value))
# gr.GeoRaster

print(ds.get_transform())

proj = epsg[str(ds.crs.to_epsg())]
print(proj)
print(ds.crs, type(ds.crs))
# {'driver': 'GTiff', 'dtype': 'uint8', 'nodata': 255.0, 'width': 100, 'height': 100, 'count': 1, 'crs': CRS.from_epsg(4326), 'transform': Affine(0.0010811939920385971, 0.0, -130.57379525218752,
#        0.0, -0.0010811939920385971, 52.39458927953412)}
