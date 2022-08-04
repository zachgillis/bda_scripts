import os
import xarray as xr
import numpy as np
import pandas as pd
import dask
import zarr

directory = '/lustre/cv/projects/casa/bda/uc4'

data = []

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    print(f)
    if 'zarr' not in f:
        continue
    zarr.consolidate_metadata(f)
    vis_xds = xr.open_dataset(f, engine="zarr")
    #row = []
    #fn = filename.split('_')
    #row.append(float(fn[7].split('.vis.zarr')[0]))
    print(len(vis_xds.DATA))
    print(dask.compute(np.mean(np.abs(vis_xds.DATA)))[0].item(0))
    #data.append(row)
    #print(row)

#avg_df = pd.DataFrame(np.array(data), columns=['t_new', 'Number of Visibilities', 'Average Amplitude of Visibilities'])
#print(avg_df)

#avg_df.to_csv('/users/zgillis/bda_scripts/avg.csv')
#avg_df.to_html('/users/zgillis/bda_scripts/avg.html')