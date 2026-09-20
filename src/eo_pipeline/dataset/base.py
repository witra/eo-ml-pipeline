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
    Construct and save a spatially aligned X/Y dataset.

    Parameters
    ----------
    x : str, Path, or xr.Dataset
        Input feature dataset. Paths are expected to point to Zarr data.
    y : str, Path, or xr.DataArray
        Input label data. Paths are expected to point to raster data.
    pad : int, default=3
        Number of pixels removed from each spatial edge after alignment.
    chunks : dict, optional
        Chunk sizes for the resulting dataset. Defaults to 256x256.
    is_save_tif : bool, default=False
        Whether to additionally save the merged dataset as GeoTIFF.
    save_dir : str, default="./"
        Output directory.
    **kwargs
        Additional arguments passed to ``save_xarray``. Must include
        ``nodata`` for the label data.

    Returns
    -------
    tuple[xr.Dataset, str]
        The merged dataset and path to the saved Zarr dataset.
    """
    if chunks is None:
        chunks = {"x": 256, "y": 256}
    if isinstance(x, (str, Path)):
        x_ds = xr.open_zarr(x)
    elif isinstance(x, xr.Dataset):
        x_ds = x
    else:
        raise TypeError(f"Unsupported type: {type(x)}")

    if isinstance(y, (str|Path)):
        y_da = rxr.open_rasterio(y)
        basename = kwargs.get('basename', os.path.basename(y).split(".")[0])
    elif isinstance(y, xr.DataArray):
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
        

