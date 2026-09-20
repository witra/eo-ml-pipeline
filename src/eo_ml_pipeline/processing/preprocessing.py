
import logging

import xarray as xr

from eo_ml_pipeline.utils.io import save_xarray

logger = logging.getLogger(__name__)

def apply_cloud_mask_s2(ds:xr.Dataset, mask_class:list, drop_scl=True):
    """Mask Sentinel-2 pixels based on Scene Classification Layer values.

    Parameters
    ----------
    ds : xarray.Dataset
        Sentinel-2 dataset containing an ``SCL`` variable.
    mask_class : list
        SCL classes to mask.
    drop_scl : bool, default=True
        Whether to remove the ``SCL`` variable after masking.

    Returns
    -------
    xarray.Dataset
        Dataset with selected SCL classes masked.
    """
    mask = ds['SCL'].isin(mask_class)
    ds = ds.where(~mask)
    if drop_scl:
        ds = ds.drop_vars("SCL")
    return ds

def calculate_median_composite(ds:xr.Dataset, dim='time'):
    """Calculate a median composite along a dimension.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset.
    dim : str, default="time"
        Dimension along which the median is calculated.

    Returns
    -------
    xarray.Dataset
        Median composite with the specified dimension reduced.
    """
    ds = ds.median(dim=dim, skipna=True)
    return ds

def scale_reflectance_s2(ds:xr.Dataset):
    """Scale Sentinel-2 reflectance values to unit reflectance.

    Parameters
    ----------
    ds : xarray.Dataset
        Sentinel-2 reflectance dataset.

    Returns
    -------
    xarray.Dataset
        Dataset with reflectance values divided by 10,000.
    """
    return ds/10_000

def apply_preprocessing_s2_base(ds:xr.Dataset, 
                                scl_mask_class=(0, 1, 3, 8, 9, 10, 11), 
                                **kwargs):
    """Apply the base Sentinel-2 preprocessing workflow.

    Applies SCL cloud masking, median compositing, and reflectance scaling.
    Optionally saves the preprocessed dataset as Zarr.

    Parameters
    ----------
    ds : xarray.Dataset
        Sentinel-2 dataset containing ``SCL``.
    scl_mask_class : tuple, default=(0, 1, 3, 8, 9, 10, 11)
        SCL classes to mask.
    **kwargs
        Additional options. ``save_preprocessed``, ``save_dir``, and
        ``basename`` control optional Zarr output.

    Returns
    -------
    tuple
        Preprocessed dataset and the output Zarr path, or ``None`` if
        the dataset was not saved.
    """
    ds = apply_cloud_mask_s2(ds, scl_mask_class)
    ds = calculate_median_composite(ds)
    ds = scale_reflectance_s2(ds)
    zarr_path = None
    if kwargs.get('save_preprocessed', None):
        zarr_path = save_xarray(ds, kwargs['save_dir'], kwargs['basename'], 'zarr')
    return ds, zarr_path

def apply_preprocessing(platform, **kwargs):
    """Apply preprocessing for a supported satellite platform.

    Parameters
    ----------
    platform : str
        Satellite platform identifier.
    **kwargs
        Arguments passed to the platform-specific preprocessing function.

    Returns
    -------
    tuple or None
        Preprocessed dataset and output path for supported platforms.
    """
    if 'S2' in platform:
        return apply_preprocessing_s2_base(**kwargs)
    else: 
        logger.warning(f'The {platform} is not available yet. Please check the available platforms')

