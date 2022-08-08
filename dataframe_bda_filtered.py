import os
import xarray as xr
import numpy as np
import pandas as pd
import dask

directory = '/lustre/cv/projects/casa/jsteeb/uc4/bda_data'

data = []

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    vis_xds = xr.open_zarr(f)

    max_bl = 13590.4688491672
    mask = dask.compute(np.sqrt(np.sum((vis_xds.UVW) ** 2)) < max_bl)

    vis_xds_filtered = vis_xds.where(mask, drop=True)
    print(1)
    print(vis_xds)
    print(2)
    print(vis_xds_filtered)
    row = []
    fn = filename.split('_')
    row.append(float(fn[7]))
    row.append(float(fn[9].split('.vis.zarr')[0]))
    row.append(len(vis_xds.DATA))
    row.append(dask.compute(((np.abs(vis_xds.DATA)).weighted(vis_xds.WEIGHT)).mean(skipna=True))[0].item(0))
    data.append(row)
    print(row)

bda_df = pd.DataFrame(np.array(data), columns=['Decorrelation Factor', 'Max. Samples Averaged', 'Number of Visibilities', 'Average Amplitude of Visibilities'])
print(bda_df)

bda_df.to_csv('/users/zgillis/bda_scripts/bda.csv')
bda_df.to_html('/users/zgillis/bda_scripts/bda.html')