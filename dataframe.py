import os
import xarray as xr

directory = '/lustre/cv/projects/casa/jsteeb/uc4/bda_data'
 
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    vis_xds = xr.open_zarr(filename)
    print(vis_xds.DATA)