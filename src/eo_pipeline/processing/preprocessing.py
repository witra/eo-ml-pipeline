
import logging

import xarray as xr

from eo_pipeline.utils.io import save_xarray

logger = logging.getLogger(__name__)

def apply_cloud_mask_s2(ds:xr.Dataset, mask_class:list, drop_scl=True):
    mask = ds['SCL'].isin(mask_class)
    ds = ds.where(~mask)
    if drop_scl:
        ds = ds.drop_vars("SCL")
    return ds

def calculate_median_composite(ds:xr.Dataset, dim='time'):
    ds = ds.median(dim=dim, skipna=True)
    return ds

def scale_reflectance_s2(ds:xr.Dataset):
    return ds/10_000

def apply_preprocessing_s2_base(ds:xr.Dataset, 
                                scl_mask_class=(0, 1, 3, 8, 9, 10, 11), 
                                **kwargs):
    ds = apply_cloud_mask_s2(ds, scl_mask_class)
    ds = calculate_median_composite(ds)
    ds = scale_reflectance_s2(ds)
    zarr_path = None
    if kwargs.get('save_preprocessed', None):
        zarr_path = save_xarray(ds, kwargs['save_dir'], kwargs['basename'], 'zarr')
    return ds, zarr_path

def apply_preprocessing(platform, **kwargs):
    if 'S2' in platform:
        return apply_preprocessing_s2_base(**kwargs)
    else: 
        logger.warning(f'The {platform} is not available yet. Please check the available platforms')

