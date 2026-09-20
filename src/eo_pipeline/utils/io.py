import logging
import xarray as xr
import rioxarray as rio

logger = logging.getLogger(__name__)

def save_xarray(ds, save_dir, filename, save_format='zarr', **kwargs):
    """Save an xarray dataset as Zarr or Cloud Optimized GeoTIFF.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to save.
    save_dir : str
        Directory where the dataset is saved.
    filename : str
        Output filename without the file extension.
    save_format : {"zarr", "tif"}, default="zarr"
        Output format.
    **kwargs
        Additional options. For Zarr, ``encoding`` can be provided.

    Returns
    -------
    str
        Path to the saved dataset, or ``None`` if the format is unsupported.
    """
    if save_format == 'zarr':
        path = f'{save_dir}/{filename}.zarr'
        encoding = kwargs.get("encoding", None)
        ds.to_zarr(path, mode="w", consolidated=False, zarr_format=3, encoding=encoding)
        return path
    elif save_format == 'tif':
        path = f'{save_dir}/{filename}.tif'
        ds.rio.to_raster(path, driver="COG", compress="ZSTD", blocksize=512, overview_resampling="nearest")
        return path
    else:
        logger.info(f'the save_format {save_format} is not available yet')