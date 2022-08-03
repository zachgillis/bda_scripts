import os
import xarray as xr
import numpy as np
import pandas as pd
import dask

directory = '/lustre/cv/projects/casa/jsteeb/uc4'

data = []

for filename in ['small_ngvla_cont_dec_45.0_no_noise.vis.zarr']:
    f = os.path.join(directory, filename)
    print(f)
    vis_xds = xr.open_zarr(f, consolidated = False)
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