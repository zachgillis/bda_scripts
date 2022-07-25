import os
import xarray as xr
import numpy as np
import pandas as pd

directory = '/lustre/cv/projects/casa/jsteeb/uc4/bda_data'

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    vis_xds = xr.open_zarr(f)
    print(vis_xds)
    #print(len(vis_xds.DATA))
    #print(np.abs(vis_xds.DATA))
    fn = filename.split('_')
    print(fn)
