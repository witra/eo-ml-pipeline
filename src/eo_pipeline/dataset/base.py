import logging
import os
from pathlib import Path

import rioxarray as rxr
import xarray as xr

from eo_pipeline.utils.io import save_xarray

logger = logging.getLogger(__name__)

def construct_xy(x:str|Path|xr.Dataset, 
                 y:str|Path|xr.DataArray, 
                 pad=3, 
                 chunks=None, 
                 is_save_tif=False, 
                 save_dir="./", 
                 **kwargs):
    """ 
    x: "file.zarr"
    y: "file.tif"
    """
    if chunks is None:
        chunks = {"x": 256, "y": 256}
    if isinstance(x, (str|Path)):
        x_ds = xr.open_zarr(x)
    elif isinstance(x, xr.Dataset):
        x_ds = x
    else:
        raise TypeError(f"Unsupported type: {type(x)}")

    if isinstance(y, (str|Path)):
        y_da = rxr.open_rasterio(y)
        basename = kwargs.get('basename', os.path.basename(y).split(".")[0])
    elif isinstance(x, xr.DataArray):
        y_da = y
        basename = kwargs.get('basename', 'xy_pair')
    else:
        raise TypeError(f"Unsupported type: {type(y)}")
     
    ref_crs = y_da.rio.crs
    x_ds = x_ds.rio.write_crs(ref_crs)
    x_ds = x_ds.rio.reproject_match(y_da)
    x_ds = x_ds.squeeze(drop=True)
    y_da = y_da.squeeze(drop=True).rename('label')
    y_da = y_da.rio.write_nodata(kwargs['nodata'])
    merged_ds = xr.merge([x_ds, y_da])
    merged_ds = merged_ds.isel(y=slice(pad, -pad), x=slice(pad, -pad))
    merged_ds = merged_ds.chunk(chunks)
    zarr_path = save_xarray(merged_ds, save_dir, basename, 'zarr', **kwargs)
    if is_save_tif:
        save_xarray(merged_ds, save_dir, basename, 'tif', nodata=kwargs['nodata'])
    return merged_ds, zarr_path
        

