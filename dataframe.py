import os
import xarray as xr
import numpy as np
import pandas as pd
import dask

directory = '/lustre/cv/projects/casa/jsteeb/uc4/bda_data'

data = []
count=0

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    vis_xds = xr.open_zarr(f)
    row = []
    fn = filename.split('_')
    row.append(float(fn[7]))
    row.append(float(fn[9].split('.vis.zarr')[0]))
    row.append(len(vis_xds.DATA))
    row.append(dask.compute(np.mean(np.abs(vis_xds.DATA)))[0].item(0))
    data.append(row)
    print(row)
    count += 1
    if count == 2:
        break


bda_df = pd.DataFrame(np.array(data), columns=['Decorrelation Factor', 'Max. Samples Averaged', 'Number of Visibilities', 'Average Amplitude of Visibilities'])
print(df)

bda_df.to_csv('/users/zgillis/bda_scripts/bda.csv')
bda_df.to_html('/users/zgillis/bda_scripts/bda.html')